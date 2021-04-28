package org.student.spark.source;

import org.student.spark.api.BaseSource;
import org.student.spark.common.CommonUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import za.co.absa.abris.config.AbrisConfig;
import za.co.absa.abris.config.FromAvroConfig;

import java.util.HashMap;
import java.util.Map;

import static org.student.spark.common.CommonUtils.getCommonConfig;
import static org.apache.spark.sql.functions.col;
import static za.co.absa.abris.avro.functions.from_avro;


public class Kafka extends BaseSource {

    final private Map<String, String> kafkaParams = new HashMap<>();
    private String topics;
    final static private String consumerPrefix = "consumer.";
    private String brokers;
    // private String schema;
    private String schemaRegistryServer;
    private boolean isStreaming = true;
    private Dataset<Row> df;
    private boolean plainText = false;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

    @Override
    public boolean checkConfig() {
        return false;
    }

    @Override
    public void prepare(SparkSession spark) {

        topics = config.getString("topics");
        brokers = config.getString("kafka.bootstrap.servers");
        result_table_name = config.getString("result_table_name");
        plainText = CommonUtils.getBooleanWithDefault(config, "plainText", false);


        Config consumerConfig = getCommonConfig(config, consumerPrefix, false);
        consumerConfig.entrySet().forEach(
                entry -> kafkaParams.put(entry.getKey(), String.valueOf(entry.getValue().unwrapped()))
        );

        logger.info("kafka paras: {}", kafkaParams);
        isStreaming = CommonUtils.getBooleanWithDefault(config, "isStreaming", true);

        // this.schema = CommonUtils.getStringWithDefault(config, "schema", "");
        String defaultSchemaServer = "defaults.kafka.schemaRegistryServer";
        String defaultSchemaServerValue = CommonUtils.getStringWithDefault(config, defaultSchemaServer, "");
        this.schemaRegistryServer = CommonUtils.getStringWithDefault(config, "schemaRegistryServer", defaultSchemaServerValue);

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }

    }

    @Override
    public void process(SparkSession spark) {

             if (isStreaming) {
                DataStreamReader dsr = spark.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", brokers)
                        .option("subscribe", topics)
                        .options(kafkaParams);

                CommonUtils.setDataStreamReaderOptions(config, dsr);
                df = dsr.load();

            } else {
                DataFrameReader reader = spark.read()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", brokers)
                        .option("subscribe", topics)
                        .options(kafkaParams);

                CommonUtils.setReaderOptions(config, reader);
                df = reader.load();
            }

            if (plainText || this.schemaRegistryServer.isEmpty()) {
                df = df.selectExpr("offset as V_OFFSET", "timestamp as V_TIMESTAMP", "cast(value as string) as value");
            } else {
                FromAvroConfig fromAvroConfig = AbrisConfig.fromConfluentAvro()
                        .downloadReaderSchemaByLatestVersion()
                        .andTopicNameStrategy(topics, false)
                        .usingSchemaRegistry(this.schemaRegistryServer);

                df = df.select(col("offset").as("V_OFFSET"),
                        col("timestamp").as("V_TIMESTAMP"),
                        from_avro(col("value"), fromAvroConfig).alias("__temp__"))
                        .select("V_OFFSET", "V_TIMESTAMP", "__temp__.*");
            }

             df.createOrReplaceTempView(result_table_name);

    }

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), result_table_name);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }
}

