package org.student.spark.sink;

import org.student.spark.api.BaseSink;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.DataFlowException;
import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import za.co.absa.abris.config.AbrisConfig;
import za.co.absa.abris.config.ToAvroConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static za.co.absa.abris.avro.functions.to_avro;
import static org.apache.spark.sql.avro.functions.to_avro;

public class Kafka extends BaseSink {

    final private Map<String, String> kafkaParams = new HashMap<>();
    private String topics;
    final static private String producerPrefix = "producer.";
    private String brokers;
    private String schemaRegistryServer;
    private boolean isStreaming = true;
    private Dataset<Row> df;
    private String checkpointLocation;
    private boolean plainText = false;
    StreamingQuery query = null;

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
        table = config.getString("table");
        plainText = CommonUtils.getBooleanWithDefault(config, "plainText", false);

        Config producerConfig = CommonUtils.getCommonConfig(config, producerPrefix, false);
        producerConfig.entrySet().forEach(
                entry -> kafkaParams.put(entry.getKey(), String.valueOf(entry.getValue().unwrapped()))
        );

        logger.info("kafka paras: {}", kafkaParams);
        isStreaming = CommonUtils.getBooleanWithDefault(config, "isStreaming", true);

        String defaultSchemaServer = "defaults.kafka.schemaRegistryServer";
        String defaultSchemaServerValue = CommonUtils.getStringWithDefault(config, defaultSchemaServer, "");
        this.schemaRegistryServer = CommonUtils.getStringWithDefault(config, "schemaRegistryServer", defaultSchemaServerValue);
    }

    @Override
    public void process(SparkSession spark) {

        if (spark.catalog().tableExists(table)) {
            df = spark.table(table);
        } else {
            throw new DataFlowException("not found table for kafka " + table);
        }
        if (!this.schemaRegistryServer.isEmpty()) {
            //todo register schema in schemaRegistry use df's schema info
            ToAvroConfig toAvroConfig = AbrisConfig
                    .toConfluentAvro()
                    .downloadSchemaByLatestVersion()
                    .andTopicNameStrategy(topics, false)
                    .usingSchemaRegistry(this.schemaRegistryServer);
            df = df.select(to_avro(struct(col("*")), toAvroConfig).alias("value"));

        } else {
            if (plainText) {
                df = df.selectExpr("cast(value as string) as value");
            } else {
                df = df.select(to_avro(struct(col("*"))).as("value"));
            }
        }


        if (isStreaming) {
            DataStreamWriter<Row> dsw = df.writeStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", brokers)
                    .option("topic", topics)
                    .options(kafkaParams);

            checkpointLocation = config.getString("checkpointLocation");
            dsw = dsw.option("checkpointLocation", checkpointLocation);
            CommonUtils.setDataStreamWriterOptions(config, dsw);
            try {
                query = dsw.start();
            } catch (Exception ex) {
                throw new DataFlowException(ex);
            }
        } else {
            DataFrameWriter<Row> writer = df.write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", brokers)
                    .option("topic", topics)
                    .options(kafkaParams);

            CommonUtils.setWriterOptions(config, writer);
            writer.save();
        }

    }

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), table);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }
}

