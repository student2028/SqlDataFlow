package org.student.spark;

import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.student.spark.common.CommonUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// For applications using application.{conf,json,properties},
// system properties can be used to force a different
// config source (e.g. from command line -Dconfig.file=path/to/config-file):

public class SqlDataFlow {

    private static final Logger logger = LoggerFactory.getLogger(SqlDataFlow.class);


    public static void main(String[] args) {

        if (!System.getProperties().containsKey("config.file")) {
            logger.error("you should specify the app config file first");
            System.exit(1);
        }
        if (args.length < 1) {
            logger.error("you should specify the config file first");
            System.exit(2);
        }
        CommonUtils.prepareConfigFromCos();
        String configFile = args[0];
        ConfigParser configParser = new ConfigParser(configFile);

        SparkConf sparkConf = CommonUtils.createSparkConf(configParser);
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        //set logLevel
        Config config = configParser.getConfig();
        String level = CommonUtils.getStringWithDefault(config, "spark.log.level", "INFO");
        spark.sparkContext().setLogLevel(level);

        final ExecutorService executorService = Executors.newFixedThreadPool(10);
        configParser.getPipelines().forEach(pipeline -> executorService.submit(() -> pipeline.start(spark)));

        boolean isStreaming = CommonUtils.getBooleanWithDefault(config, "spark.structured.streaming", false);
        if(isStreaming) {
            final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                logger.info("Start to stop the app without Active StreamingQuery");
                if (spark.streams().active().length < 1) System.exit(100);
            }, 120, 300, TimeUnit.SECONDS);

            try {
                spark.streams().awaitAnyTermination();
            } catch (StreamingQueryException e) {
                logger.error(e.getMessage());
            }
        } else {
            spark.stop();
        }
    }

}
