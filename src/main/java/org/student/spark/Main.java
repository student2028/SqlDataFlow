package org.student.spark;

import org.student.spark.common.CommonUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;

import static org.student.spark.common.CommonUtils.batchProcess;

// For applications using application.{conf,json,properties},
// system properties can be used to force a different
// config source (e.g. from command line -Dconfig.file=path/to/config-file):

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);


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
        String level = CommonUtils.getStringWithDefault(configParser.getConfig(), "spark.log.level", "INFO");
        spark.sparkContext().setLogLevel(level);

        CommonUtils.batchProcess(spark, configParser);

        spark.stop();
    }

}
