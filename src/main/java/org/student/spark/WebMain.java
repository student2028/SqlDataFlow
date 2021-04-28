package org.student.spark;

import com.fasterxml.jackson.core.type.TypeReference;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.Constants;
import org.student.spark.common.DataFlowException;
import org.student.spark.common.JsonUtils;
import org.student.spark.sql.SQL2HOCONDriver;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.util.Headers;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.WriterAppender;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.student.spark.common.CommonUtils.batchProcess;
import static io.undertow.Handlers.*;


public class WebMain {

    static final Logger logger = Logger.getLogger(WebMain.class);
    static final String responseJsonTemplate = "{\"data\":%s,\"schema\":%s}";
    static final ExecutorService executorService = Executors.newFixedThreadPool(10);
    static final String SQLQUERY = "sqlquery";
    static final String ENV = "env";

    public static void main(String[] args) {
        //read config file from s3 object store
        //CommonUtils.prepareConfigFromCos();
        ConfigParser configParser;
        if (args.length > 0) {
            configParser = new ConfigParser(args[0]);
        } else {
            configParser = new ConfigParser();
        }
        SparkConf sparkConf = CommonUtils.createSparkConf(configParser);
        if (!sparkConf.contains("spark.master")) {
            sparkConf.set("spark.master", "local");
            sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
        }
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        String level = CommonUtils.getStringWithDefault(configParser.getConfig(),"spark.log.level","INFO");
        spark.sparkContext().setLogLevel(level);

        WebMain.setupWebServer(configParser.getConfig(), spark);
        if (args.length > 0) {
            CommonUtils.batchProcess(spark, configParser);
        }
    }

    public static void setupWebServer(Config config, SparkSession spark) {

        int port = CommonUtils.getIntWithDefault(config, "app.web.server.port", 8080);
        int workThreads = CommonUtils.getIntWithDefault(config, "app.web.worker.threads", 10);
        int ioThreads = CommonUtils.getIntWithDefault(config, "app.web.io.threads", 10);
        // String webResourceBasePath = CommonUtils.getStringWithDefault(config, "app.web.ui.path", "/home/spark/mnt/web/ui/");
        Undertow server = Undertow.builder()
                .addHttpListener(port, "0.0.0.0")
                .setWorkerThreads(workThreads)
                .setIoThreads(ioThreads)
                .setHandler(
                        setUpPathHandler(config, spark)
                ).build();
        server.start();
        logger.info("Bound WebUI to 0.0.0.0, and started http://localhost:" + port);
    }

    static HttpHandler setUpPathHandler(Config config, SparkSession spark) {
        return path().addPrefixPath("/",
                resource(new ClassPathResourceManager(WebMain.class.getClassLoader(), ""))
                //resource(new PathResourceManager(Paths.get(webResourceBasePath)))
        ).addExactPath("/sdfsql", sparkdataflowHandler(spark, config))
                .addExactPath("/sparksql", sparksqlHandler(spark))
                .addExactPath("/rundf", rundfHandler(spark))
                .addExactPath("/log", logPodWSHandler())
                .addExactPath("/stopquery", stopQueryHandler(spark));
    }


    static void logLocalLog(WebSocketChannel channel) {
        PipedReader reader;
        Writer writer;
        Logger root = Logger.getRootLogger();
        try {
            String hostAddress = InetAddress.getLocalHost().getHostAddress();
            MDC.put("ip", hostAddress);
            Appender appender = root.getAppender("WA");
            reader = new PipedReader();
            writer = new PipedWriter(reader);
            ((WriterAppender) appender).setWriter(writer);
            executorService.submit(() -> {
                String line;
                try (BufferedReader br = new BufferedReader(reader)) {
                    while ((line = br.readLine()) != null) {
                        WebSockets.sendText(line + "<br/>", channel, null);
                    }
                } catch (Exception ex) {
                    logger.warn(ex.getMessage());
                }
            });
        } catch (Exception ex) {
            WebSockets.sendText("Exceptions:" + ex.getMessage(), channel, null);
        }
    }


    static HttpHandler logPodWSHandler() {

        return websocket((exchange, channel) -> {
            Map<String, List<String>> paras = exchange.getRequestParameters();
            logger.info(paras);
            String ns = paras.get("ns").get(0);
            String podName = paras.get("pod").get(0);
            logger.info(podName);
            if (podName.equalsIgnoreCase("localhost")) logLocalLog(channel);
            else {
                try (KubernetesClient client = new DefaultKubernetesClient()) {
                    LogWatch watch = client.pods().inNamespace(ns).withName(podName).tailingLines(10).watchLog();
                    InputStream inputstream = watch.getOutput();
                    executorService.submit(() -> {
                        String line;
                        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputstream))) {
                            while ((line = br.readLine()) != null) {
                                WebSockets.sendText(line + "<br/>", channel, null);
                            }
                        } catch (Exception ex) {
                            logger.warn(ex.getMessage());
                        }
                    });
                } catch (Exception ex) {
                    WebSockets.sendText("Exceptions:" + ex.getMessage(), channel, null);
                }
            }
        });
    }

    private static HttpHandler rundfHandler(SparkSession spark) {
        return baseHandler((exchange, paras) -> {
            String sql = paras.get(SQLQUERY).toString();
            if (paras.containsKey("transql")) {
                logger.info("sql:" + sql);
                sql = SQL2HOCONDriver.getJson(sql);
                logger.info("sql to json :" + sql);
            }
            ConfigParser cp = new ConfigParser().loadString(sql);

            Future task = executorService.submit(() -> {
                        CommonUtils.batchProcess(spark, cp);
                    }
            );

            try {
                task.get();
            } catch (ExecutionException | InterruptedException ex) {
                logger.error(ex.getMessage());
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/text");
                exchange.getResponseSender().send(ex.getMessage());
            }
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/text");
            exchange.getResponseSender().send("job has submitted to spark, please check later");
        });
    }

    private static HttpHandler sparksqlHandler(SparkSession spark) {

        return baseHandler((exchange, paras) -> {
            String sql = paras.get(SQLQUERY).toString();
            String limit = paras.get("limit").toString();
            Dataset<Row> df = spark.sql(sql);
            jsonRender(exchange, getResponseJsonFromDataFrame(df, limit));
        });
    }


    private static HttpHandler stopQueryHandler(SparkSession spark) {
        return baseHandler((exchange, paras) -> {
            String sql = paras.get(SQLQUERY).toString();
            try {
                spark.streams().get(StringUtils.trim(sql)).stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
            jsonRender(exchange, "stop query finished");
        });
    }

    private static String getResponseJsonFromDataFrame(Dataset<Row> df, String limit) {
        int result_row_count = CommonUtils.extractNumFromString(limit, 10);
        result_row_count = Math.min(result_row_count, 10000);
        String data = Arrays.stream((String[]) df.toJSON().take(result_row_count))
                .collect(Collectors.joining(",", "[", "]"));
        String schema = Arrays.stream(df.schema().fields()).map(f -> "{\"name\":\"" + f.name() + "\",\"type\":\""
                + f.dataType() + "\"}")
                .collect(Collectors.joining(",", "[", "]"));
        return "{\"data\":" + data + ",\"schema\":" + schema + "}";
    }

    private static void validateUser(Map<String, Object> paras) {
        String secretKey = paras.get("secret").toString();
        boolean isValidUser = verifyPermission(secretKey);
        if (!isValidUser) {
            throw new DataFlowException("Not a valid user,please input secretkey");
        }
    }


    public static Config makeConfig(String env, String sql, String result_table_name) {
        Config conf = ConfigFactory.empty()
                .withValue("processor.sql.inferSource", ConfigValueFactory.fromAnyRef(true))
                .withValue("processor.sql.sql", ConfigValueFactory.fromAnyRef(sql))
                .withValue("processor.sql.result_table_name", ConfigValueFactory.fromAnyRef(result_table_name))
                .withValue("processor.sql.sourceTag", ConfigValueFactory.fromAnyRef("meta"))
                .withValue("bigdata.environment", ConfigValueFactory.fromAnyRef(env));

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        logger.info("[INFO] parsed config string: " + conf.root().render(options));

        return conf;
    }


    public static boolean verifyPermission(String key) {
        return true;
    }

    private static void jsonRender(HttpServerExchange exchange, String responseJson) {
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send(responseJson);
    }

    static Map<String, Object> getParas(String message) {
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        return JsonUtils.serializer().fromJson(message, typeRef);
    }


    static HttpHandler sparkdataflowHandler(SparkSession spark, Config config) {
        return baseHandler((exchange, paras) -> {
            String sql = paras.get(SQLQUERY).toString();
            String limit = paras.get("limit").toString();
            String env = paras.get(ENV).toString();
            String result_table_name = UUID.randomUUID().toString().replace("-", "");
            Config jobConf = makeConfig(env, sql, result_table_name).withFallback(config);
            CommonUtils.batchProcess(spark, jobConf);
            Dataset<Row> df = spark.table(result_table_name);
            jsonRender(exchange, getResponseJsonFromDataFrame(df, limit));
        });
    }

    static HttpHandler baseHandler(BiConsumer<HttpServerExchange, Map<String, Object>> worker) {
        return exchange -> {
            try {
                exchange.getRequestReceiver().receiveFullString((ex, message) ->
                {
                    try {
                        Map<String, Object> paras = getParas(message);
                        validateUser(paras);
                        if (paras.containsKey(ENV)) {
                            String env = paras.get(ENV).toString();
                            System.setProperty(Constants.BIGDATA_ENV, env);
                        }
                        worker.accept(exchange, paras);
                    } catch (Exception ex2) {
                        renderException(exchange, ex2);
                    }
                });
            } catch (Exception ex) {
                renderException(exchange, ex);
            }
        };

    }

    static void renderException(HttpServerExchange exchange, Exception ex) {
        logger.error("error", ex);
        String data = "[{\"exception\":" + JsonUtils.serializer().writeValueAsString(ex.getMessage()) + "}]";
        String responseJson = String.format(responseJsonTemplate, data, "\"\"");
        logger.info(responseJson);
        jsonRender(exchange, responseJson);
    }

}