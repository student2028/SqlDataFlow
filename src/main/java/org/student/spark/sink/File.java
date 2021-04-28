package org.student.spark.sink;

import org.student.spark.api.BaseSink;
import org.student.spark.common.CommonUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class File extends BaseSink {

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s",getClass().getCanonicalName(),table);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    @Override
    public void process(SparkSession spark) {

        Dataset<Row> df = spark.table(table);
        DataFrameWriter writer = df.write().format(format).mode(save_mode);
        CommonUtils.setWriterOptions(config, writer);
        writer.save(path);
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public boolean checkConfig() {
        return true;
    }

    @Override
    public void prepare(SparkSession spark) {
        format  =  CommonUtils.getStringWithDefault(config,"format", "parquet");
        table = config.getString("table");
        path = config.getString("path");
        save_mode = CommonUtils.getStringWithDefault(config, "savemode", save_mode);

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }
    }

 }
