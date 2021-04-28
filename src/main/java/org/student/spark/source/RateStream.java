package org.student.spark.source;

import org.student.spark.api.BaseSource;
import org.student.spark.common.CommonUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

public class RateStream extends BaseSource {

    private int numPartitions = 1;
    private long rampUpTimeSeconds = 0L;
    private long rowsPerSecond = 1L;

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
        numPartitions = CommonUtils.getIntWithDefault(config, "numPartitions", numPartitions);
        rampUpTimeSeconds = CommonUtils.getLongWithDefault(config, "rampUpTimeSeconds", rampUpTimeSeconds);
        rowsPerSecond = CommonUtils.getLongWithDefault(config, "rowsPerSecond", rowsPerSecond);

        result_table_name = config.getString("result_table_name");

    }

    @Override
    public void process(SparkSession spark) {

        DataStreamReader dsr = spark.readStream().format("rate")
                .option("numPartitions", numPartitions)
                .option("rampUpTimeSeconds", rampUpTimeSeconds)
                .option("rowsPerSecond", rowsPerSecond);

        CommonUtils.setDataStreamReaderOptions(config, dsr);

        dsr.load().createOrReplaceTempView(result_table_name);

    }

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), result_table_name);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }
}
