package org.student.spark.sink;

import org.student.spark.api.BaseSink;
import org.student.spark.common.CommonUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.UUID;

public class Console extends BaseSink {

    private int limit=100;
    private boolean isStreaming = false;
    private String streamingOutputMode;
    private String triggerType;
    private String checkpointLocation="";

    StreamingQuery query = null;

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), table);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    @Override
    public void process(SparkSession spark) {

        Dataset<Row> df = spark.table(table);
        df.printSchema();
        if (!isStreaming) {
            limit = limit == -1 ? Integer.MAX_VALUE : limit;
            if (format.equalsIgnoreCase("json")) {
                df.toJSON().show(limit, false);
                logger.info(df.toJSON().showString(limit,30,false));
            } else {
                df.show(limit, false);
                logger.info(df.showString(limit,30,false));
            }
        } else {

            DataStreamWriter<Row> writer = df.writeStream()
                    .format("console")
                    .option("truncate","false")
                    .option("numRows",limit)
                    .outputMode(streamingOutputMode);

            if (!checkpointLocation.isEmpty()) {
                writer = writer.option("checkpointLocation", checkpointLocation);
            }

            writer = CommonUtils.setDataStreamTrigger(writer, config, triggerType);
            try {
               query = writer.start();
            } catch (Exception e) {
                logger.error("process", e);
            }
        }

    }

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
        table = config.getString("table");
        isStreaming = CommonUtils.getBooleanWithDefault(config, "isStreaming", false);

        if (!isStreaming) {
            limit = CommonUtils.getIntWithDefault(config, "limit", 20);
            format = CommonUtils.getStringWithDefault(config, "format", "plain");
        } else {
            streamingOutputMode = CommonUtils.getStringWithDefault(config, "streaming_output_mode", "Append");
            triggerType = CommonUtils.getStringWithDefault(config, "trigger_type", "default");
            checkpointLocation = CommonUtils.getStringWithDefault(config, "checkpointLocation", "/tmp/sdf/"+ UUID.randomUUID());
        }
        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }
    }
}
