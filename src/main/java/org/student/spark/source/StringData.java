package org.student.spark.source;

import org.student.spark.api.BaseSource;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.DataFlowException;
import com.typesafe.config.Config;
import org.apache.spark.sql.*;

import java.util.Arrays;

/*
 *
 * Just used for test
 */
public class StringData extends BaseSource {

    private String data;
    private String format;

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), result_table_name);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    @Override
    public void process(SparkSession spark) {
        Dataset<Row> df = null;
        if (format.equalsIgnoreCase("json")) {
            Dataset<String> ds = spark.createDataset(Arrays.asList(data), Encoders.STRING());
            df = spark.read().json(ds);
        } else if (format.equalsIgnoreCase("csv")) {
            Dataset<String> ds = spark.createDataset(Arrays.asList(data.split("\n")), Encoders.STRING());
            DataFrameReader reader = spark.read();
            CommonUtils.setReaderOptions(config, reader);
            df = reader.option("header", "true").csv(ds);
        } else {
            throw new DataFlowException("StringData only support json and csv format!");
        }
        df.createOrReplaceTempView(result_table_name);
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

        format = CommonUtils.getStringWithDefault(config, "format", "json");
        data = config.getString("data");
        result_table_name = CommonUtils.getStringWithDefault(config, "result_table_name", "stringdata");

    }
}
