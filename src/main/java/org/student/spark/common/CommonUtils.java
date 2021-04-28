package org.student.spark.common;

import org.student.spark.ConfigParser;
import org.student.spark.api.BaseProcessor;
import org.student.spark.api.BaseSink;
import org.student.spark.api.BaseSource;
import org.student.spark.api.Plugin;
import com.typesafe.config.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.HtmlEmail;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map$;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class CommonUtils {

    private static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

    public static String getStringSafe(Config config, String key) {
        return getStringWithDefault(config, key, "");
    }

    public static String getStringWithDefault(Config config, String key, String value) {
        return config.hasPath(key) ? config.getString(key) : value;
    }

    public static String getStringWithPrefix(Config config, String key, String prefix) {
        return config.hasPath(key) ? prefix + config.getString(key) : "";
    }

    public static int getIntWithDefault(Config config, String key, int value) {
        return config.hasPath(key) ? config.getInt(key) : value;
    }

    public static long getLongWithDefault(Config config, String key, long value) {
        return config.hasPath(key) ? config.getLong(key) : value;
    }

    public static boolean getBooleanWithDefault(Config config, String key, boolean value) {
        return config.hasPath(key) ? config.getBoolean(key) : value;
    }

    public static String normalizeName(String table) {
        return table == null ? null : (table.contains(".") ? table.replaceAll("\\.", "_") : table);
    }

    public static void printSQLException(SQLException ex) {
        while (ex != null) {
            logger.error("Error msg: " + ex.getMessage());
            logger.error("SQLSTATE: " + ex.getSQLState());
            logger.error("Error code: " + ex.getErrorCode());
            ex = ex.getNextException();
        }
    }

    public static String regexExtractFirst(String data, String regexp) {
        Pattern pattern = Pattern.compile(regexp);
        Matcher matcher = pattern.matcher(data);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "0";
    }

    public static List<String> regexpExtractAll(String data, String regexp) {
        Pattern pattern = Pattern.compile(regexp);
        Matcher matcher = pattern.matcher(data);
        List<String> ret = new ArrayList<>();
        while (matcher.find()) {
            ret.add(matcher.group());
        }
        return ret;
    }

    public static Integer extractNumber(String data) {
        Pattern pattern = Pattern.compile(Constants.REGEXP_NUMBER);
        Matcher matcher = pattern.matcher(data);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return 0;
    }

    public static int getStringOctetsLen(String str) {
        return str.getBytes(StandardCharsets.UTF_8).length;
    }

    public static Dataset<Row> castDataset(Dataset<Row> ds, java.util.Map<String, String> columns) {

        final String[] originalColumns = ds.columns();
        final Set<String> columnSet = new HashSet<>(originalColumns.length);
        Collections.addAll(columnSet, originalColumns);
        columnSet.retainAll(columns.keySet());

        for (String column : columnSet) {
            final String typeName = columns.get(column);
            if ("timestamp".equalsIgnoreCase(typeName)) {
                ds = ds.withColumn(column, to_timestamp(substring(col(column), 0, 26)));
            } else {
                ds = ds.withColumn(column, col(column).cast(typeName));
            }
        }
        return ds;
    }

    public static String loadFile(String sqlPath) {
        String sql;
        String regex = "(\\w+)://.*";//used to match s3 or cos path style
        if (sqlPath.matches(regex)) {
            sql = CosUtils.getFileAsString(sqlPath);
        } else {
            try {
                sql = Files.readAllLines(Paths.get(sqlPath), StandardCharsets.UTF_8)
                        .stream().collect(Collectors.joining(System.lineSeparator()));
            } catch (IOException e) {
                throw new DataFlowException("LoadSqlFile failed", e);
            }
        }
        return sql;
    }

    //print class all fields value used to debug apache common toStringBuilder
    private static Collection<Field> getAllFields(Class<?> type) {
        TreeSet<Field> fields = new TreeSet<>(
                Comparator.comparing(Field::getName).thenComparing(o -> o.getDeclaringClass().getSimpleName())
                        .thenComparing(o -> o.getDeclaringClass().getName()));
        for (Class<?> c = type; c != null; c = c.getSuperclass()) {
            fields.addAll(Arrays.asList(c.getDeclaredFields()));
        }
        return fields;
    }

    public static void printAllFields(Object obj) {
        for (Field field : getAllFields(obj.getClass())) {
            field.setAccessible(true);
            String name = field.getName();
            if (name.equalsIgnoreCase("config")
                    || name.equalsIgnoreCase("password")) {
                continue;
            }
            Object value = null;
            try {
                value = field.get(obj);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                logger.error("printAllFields", e);
            }
            //System.out.printf("%s %s.%s = %s%n", value == null ? " " : "*******", field.getDeclaringClass().getSimpleName(), name, value);
            logger.info("{} {}.{} ={}", value == null ? " " : "*******", field.getDeclaringClass().getSimpleName(), name, value);
        }
    }

    public static Column nullSafeJoin(final Dataset<Row> oldDataSet, final Dataset<Row> newDataSet) {
        final String[] newColumns = newDataSet.columns();
        final String[] oldColumns = oldDataSet.columns();
        Column col = col("d2." + newColumns[0]).eqNullSafe(col("old." + oldColumns[0]));
        for (int i = 1; i < newColumns.length; i++) {
            col = col.and(col("d2." + newColumns[i]).eqNullSafe(col("old." + oldColumns[i])));
        }
        return col;
    }

    public static Set<String> extractTables(String query, SparkSession spark) {
        LogicalPlan logical;
        try {
            logical = spark.sessionState().sqlParser().parsePlan(query);
        } catch (ParseException e) {
            throw new DataFlowException("parse failed for sql : " + query, e);
        }
        Set<String> tables = new HashSet<>();
        Set<String> alias = new HashSet<>();

        int i = 0;
        while (true) {
            if (logical.apply(i) == null) {
                tables.removeAll(alias);
                return tables;
            } else if (logical.apply(i) instanceof UnresolvedRelation) {
                String tableIdentifier = ((UnresolvedRelation) logical.apply(i)).tableName();
                tables.add(tableIdentifier);
            } else if (logical.apply(i) instanceof SubqueryAlias) {
                String aliasName = ((SubqueryAlias) logical.apply(i)).alias();
                alias.add(aliasName);
            }
            i = i + 1;
        }
    }

    public static String getK8sSecret(Config config) {
        String secretDir = config.getString(Constants.K8S_SECRETS_DIR);
        String secretFile = config.getString(Constants.K8S_SECRETS_FILE);

        return secretDir.endsWith("/") ? secretDir + secretFile : secretDir + "/" + secretFile;
    }


    public static Dataset<Row> trimDataset(Dataset<Row> ds, String trimString) {
        final Tuple2<String, String>[] dtypes = ds.dtypes();
        for (Tuple2<String, String> dtype : dtypes) {
            if (dtype._2().equalsIgnoreCase("StringType")) {
                ds = ds.withColumn(dtype._1(), trim(col(dtype._1()), trimString));
            }
        }
        return ds;
    }

    public static Config makeSourcesConfig(String sourceTag, String sql, SparkSession spark) {
        Set<String> tables = extractTables(sql, spark);
        int index = 0;
        Config config = ConfigFactory.empty();
        for (String table : tables) {
            config = config.withValue("source.cos" + index + ".table", ConfigValueFactory.fromAnyRef(table))
                    .withValue("source.cos" + index + ".tag", ConfigValueFactory.fromAnyRef(sourceTag));
            index++;
        }
        return config;
    }

    public static String getResultTableName(Config config, String defaultValue) {
        return getStringWithDefault(
                config, "result_table_name",
                defaultValue.replaceAll("\\.", "_"));
    }

    public static Config getCommonConfig(Config source, String prefix, boolean keepPrefix) {

        Map<String, String> values = new LinkedHashMap<>();
        for (Map.Entry<String, ConfigValue> entry : source.entrySet()) {
            final String key = entry.getKey();
            final String value = String.valueOf(entry.getValue().unwrapped());
            if (key.startsWith(prefix)) {
                if (keepPrefix) {
                    values.put(key, value);
                } else {
                    values.put(key.substring(prefix.length()), value);
                }
            }
        }
        return ConfigFactory.parseMap(values);
    }

    public static String extractFileNameWithoutExtension(String path) {
        Path fileName = Paths.get(path).getFileName();
        logger.debug("path and filename is {}  {} ", path, fileName);
        if (fileName == null) {
            throw new DataFlowException("Not found fileName from " + path);
        }
        return fileName.toString().replaceAll("(?<=.)[.][^.]+$", "");
    }

    public static void setReaderOptions(Config config, DataFrameReader reader) {
        Config options = CommonUtils.getCommonConfig(config, Constants.OPTIONS_PREFIX, false);
        logger.debug("reader options : {}", options);

        options.entrySet().forEach(entry -> reader.option(entry.getKey(), String.valueOf(entry.getValue().unwrapped())));
    }

    public static void setWriterOptions(Config config, DataFrameWriter writer) {
        Config options = CommonUtils.getCommonConfig(config, Constants.OPTIONS_PREFIX, false);
        logger.info("writer options : {}", options);
        options.entrySet().forEach(entry -> writer.option(entry.getKey(), String.valueOf(entry.getValue().unwrapped())));
    }

    public static Config mergeK8sSecrets(Config config) {
        boolean isEnabled = config.getBoolean(Constants.K8S_SECRETS_ENABLED);
        logger.info("use k8s secrets: {}", isEnabled);
        if (isEnabled) {
            String path = CommonUtils.getK8sSecret(config);
            Config conf = ConfigFactory.parseFile(new File(path));
            logger.info("secret file {}", path);
            config = config.withFallback(conf);
        }
        return config;
    }

    static String unwrapKey(String key) {
        return String.join(".", ConfigUtil.splitPath(key));
    }

    public static SparkConf createSparkConf(ConfigParser parser) {
        final SparkConf sparkConf = new SparkConf();
        parser.getSparkConfig().entrySet().forEach(entry ->
        {
            if (entry.getKey().startsWith("spark")) {
                sparkConf.set(unwrapKey(entry.getKey()), String.valueOf(entry.getValue().unwrapped()));
            } else {
                sparkConf.set(unwrapKey("spark." + entry.getKey()), String.valueOf(entry.getValue().unwrapped()));
            }
        });

        return sparkConf;
    }

    public static void setDataStreamReaderOptions(Config config, DataStreamReader reader) {
        Config options = CommonUtils.getCommonConfig(config, Constants.OPTIONS_PREFIX, false);
        logger.info("data stream reader options : {}", options);
        options.entrySet().forEach(entry -> reader.option(entry.getKey(), String.valueOf(entry.getValue().unwrapped())));
    }

    public static void setDataStreamWriterOptions(Config config, DataStreamWriter writer) {
        Config options = CommonUtils.getCommonConfig(config, Constants.OPTIONS_PREFIX, false);
        logger.info("data stream writer options : {}", options);
        options.entrySet().forEach(entry -> writer.option(entry.getKey(), String.valueOf(entry.getValue().unwrapped())));
    }

    public static DataStreamWriter<Row> setDataStreamTrigger(DataStreamWriter<Row> writer, Config config, String triggerType) {
        switch (triggerType) {
            case "ProcessingTime":
                writer = writer.trigger(Trigger.ProcessingTime(config.getString("interval")));
                break;
            case "OneTime":
                writer = writer.trigger(Trigger.Once());
                break;
            case "Continuous":
                writer = writer.trigger(Trigger.Continuous(config.getString("interval")));
                break;
            case "default":
                break;
        }
        return writer;
    }

    public static String getUTCNow() {
        return getUTCNow("yyyy-MM-dd HH:mm:ss.SSS");
    }

    public static String getUTCNow(String timeFormat) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeFormat);
        final ZonedDateTime zonedDateTime = ZonedDateTime.now(ZoneId.of("UTC"));
        return zonedDateTime.format(formatter);
    }

    public static int extractNumFromString(String text, int defaultValue) {
        String result = text.replaceAll("[^\\d]", "");
        return result.isEmpty() ? defaultValue : Integer.parseInt(result);
    }

    public static void prepareConfigFromCos() {
        String secretDir;
        if (System.getProperties().containsKey("secret.dir")) {
            String classPath = "/tmp/";//ClassLoader.getSystemResource("").getPath();
            logger.info("classpath is :" + classPath);
            String appPath = classPath + "application.conf";
            secretDir = System.getProperty("secret.dir");
            Config secretConf = ConfigFactory.parseFile(new File(secretDir));
            String cosAccessKey = secretConf.getString("spark.hadoop.fs.cos.service.access.key");
            String cosSecretKey = secretConf.getString("spark.hadoop.fs.cos.service.secret.key");
            String cosEndpoint = secretConf.getString("spark.hadoop.fs.cos.service.endpoint");
            CosUtils.initialize(cosAccessKey, cosSecretKey, cosEndpoint);
            String applicationPath = System.getProperty("config.file");
            CosUtils.downloadFile(applicationPath, appPath);
            System.setProperty("config.file", appPath);
        }
    }

    public static String trimEndNumber(String str) {
        return StringUtils.stripEnd(str, "0123456789");
    }

    public static String getEnv(Config config) {
        String env;
        if (config.hasPath(Constants.BIGDATA_ENV)) {
            env = config.getString(Constants.BIGDATA_ENV);
        } else if (System.getProperties().containsKey(Constants.BIGDATA_ENV)) {
            env = System.getProperty(Constants.BIGDATA_ENV);
        } else {
            throw new DataFlowException("not found env config in System.property or config!");
        }

        return env;
    }


    public static String httpGetRequest(String requestUrl) {
        int maxRetry = 5;
        int times = 1;
        boolean hasException;
        final StringBuilder result = new StringBuilder();

        do {
            try {
                final URL url = new URL(requestUrl);
                final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                try (BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String line;
                    while ((line = rd.readLine()) != null) {
                        result.append(line);
                    }
                }
                hasException = false;
            } catch (Exception ex) {
                hasException = true;
                if (times == maxRetry - 1) {
                    throw new DataFlowException(ex);
                } else if (times < maxRetry - 2) {
                    try {
                        Thread.sleep((long) (1000 * Math.pow(2, times)));
                    } catch (InterruptedException ie) {
                        //ignore
                    }
                }
                result.setLength(0);
            }
            times++;
        } while (hasException && times < maxRetry);

        return result.toString();
    }

    public static String latestSchemaInRegistry(String topicKafka, String schemaServerURI) {
        final String schemaServer = StringUtils.stripEnd(schemaServerURI, "/");
        final String versions = httpGetRequest(schemaServer + "/subjects/" + topicKafka + "-value/versions");
        String version;
        if (versions.lastIndexOf(',') > -1) {
            version = versions.substring(versions.lastIndexOf(',') + 1, versions.length() - 1);
        } else {
            version = versions.substring(1, versions.length() - 1);
        }
        final String schema = httpGetRequest(schemaServer + "/subjects/" + topicKafka + "-value/versions/" + version + "/schema");

        logger.info("{} latest version {}  schema is {}", topicKafka, version, schema);
        return schema;
    }

    public static void batchProcess(SparkSession spark, Config config) {
        ConfigParser cp = new ConfigParser(config);
        batchProcess(spark, cp);
    }

    public static void batchProcess(SparkSession spark, ConfigParser configParser) {

        List<BaseSource> sourceList = configParser.createSources();
        List<BaseProcessor> transList = configParser.createProcessors();
        List<BaseSink> sinkList = configParser.createSinks();

        boolean sourceParallel = CommonUtils.getBooleanWithDefault(configParser.getConfig(), "execution.source.parallel", false);
        boolean processorParallel = CommonUtils.getBooleanWithDefault(configParser.getConfig(), "execution.processor.parallel", false);
        boolean sinkParallel = CommonUtils.getBooleanWithDefault(configParser.getConfig(), "execution.sink.parallel", false);
        boolean isTest = CommonUtils.getBooleanWithDefault(configParser.getConfig(), "spark.streaming.test", false);

        for (Plugin plugin : sourceList) {
            plugin.prepare(spark);
        }

        for (Plugin plugin : transList) {
            plugin.prepare(spark);
        }

        for (Plugin plugin : sinkList) {
            plugin.prepare(spark);
        }
        logger.info("start to do sources components ....");

        if (sourceParallel) {
            sourceList.parallelStream().forEach(source ->
                    source.process(spark));
        } else {
            for (BaseSource source : sourceList) {
                source.process(spark);
                logger.info(source.getPluginName() + " has finished work....");
            }
        }
        logger.info("start to do processor components....");

        if (processorParallel) {
            transList.parallelStream().forEach(processor ->
                    processor.process(spark));
        } else {
            for (BaseProcessor processor : transList) {
                processor.process(spark);
                logger.info(processor.getPluginName() + " has finished work....");
            }
        }
        logger.info("start to sink components....");

        if (sinkParallel) {
            sinkList.parallelStream().forEach(sink -> sink.process(spark));
        } else {
            for (BaseSink sink : sinkList) {
                sink.process(spark);
                logger.info(sink.getPluginName() + " has finished work....");
            }
        }

        if (CommonUtils.getBooleanWithDefault(configParser.getConfig(), "spark.structured.streaming", false)) {
            try {
                if (isTest) {
                    //test case let the query control the termination
                    //spark.streams().awaitAnyTermination(6000);
                } else {
                    spark.streams().awaitAnyTermination();
                }
            } catch (StreamingQueryException e) {
                throw new DataFlowException(e);
            }
        }
    }

    //map java map to scala immutable map
    public static <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(java.util.Map<K, V> jmap) {
        List<Tuple2<K, V>> tuples = jmap.entrySet()
                .stream()
                .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

        Seq<Tuple2<K, V>> scalaSeq = JavaConverters.asScalaBuffer(tuples).toSeq();

        return (scala.collection.immutable.Map<K, V>) Map$.MODULE$.apply(scalaSeq);
    }

    public static String replaceVariable(String str, SparkSession spark) {
        String regex = "(?<=\\$\\{)(\\S+?)(?=\\})";
        String rest = str;
        List<String> varList = regexpExtractAll(str, regex);
        for (int i = 0; i < varList.size(); i++) {
            String varName = varList.get(i);
            if (spark.conf().contains(varName)) {
                rest = rest.replaceAll("\\$\\{" + varName + "\\}", spark.conf().get(varName));
            }
        }
        return rest;
    }

    public static Map<String, Object> row2JavaMap(Row row) {
        Map<String, Object> map = null;

        List<String> list = Arrays.stream(row.schema().fields())
                .map(field -> field.name()).collect(Collectors.toList());
        map = scala.collection.JavaConverters.mapAsJavaMap(
                row.getValuesMap(scala.collection.JavaConverters.asScalaBuffer(list).toSeq())
        );
        return map;
    }

    public static Dataset<Row> map2Dataframe(Map<String, Object> map, SparkSession spark) {
        List<Row> data = new ArrayList<>();
        List<Object> list = Arrays.asList(map.values()).stream().map(v -> v.toString()).collect(Collectors.toList());
        Row row = Row.fromSeq(scala.collection.JavaConverters.asScalaBuffer(list).toSeq());
        data.add(row);
        List<StructField> cols = map.keySet().stream().map(k -> DataTypes.createStructField(k, DataTypes.StringType, true)).collect(Collectors.toList());
        StructType schema = DataTypes.createStructType(cols);
        return spark.createDataFrame(data, schema);
    }

    public static void sendMail(String smtpHost, int smtpPort, String emailFrom, String emailTo, String emailSubject, String msg) {
        try {
            HtmlEmail mail = new HtmlEmail();
            mail.setHostName(smtpHost);
            mail.setSmtpPort(smtpPort);
            mail.setAuthenticator(null);

            mail.setFrom(emailFrom, "SparkDataflow EmailSender");
            for (String mailto : emailTo.split(";")) {
                mail.addTo(mailto, mailto);
            }
            mail.setSubject(emailSubject);
            mail.setHtmlMsg(msg);
            mail.send();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

    }
}