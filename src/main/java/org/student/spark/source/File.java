package org.student.spark.source;

import org.student.spark.api.BaseSource;
import org.student.spark.common.CommonUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

public class File extends BaseSource {

    private String format = "parquet"; //default format
    private String path;
    private DataFrameReader reader;
    private boolean isStreaming = false;
    private DataStreamReader streamReader;

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), result_table_name);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    @Override
    public void process(SparkSession spark) {

        Dataset<Row> df = isStreaming ? streamReader.load(path) : reader.load(path);
        df.createOrReplaceTempView(result_table_name);

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
        format = CommonUtils.getStringWithDefault(config, "format", format);
        path = config.getString("path");
        String fileNameWithoutExtension = CommonUtils.extractFileNameWithoutExtension(path);
        result_table_name = CommonUtils.getResultTableName(config, fileNameWithoutExtension);
        isStreaming = CommonUtils.getBooleanWithDefault(config, "isStreaming", isStreaming);
        if (isStreaming) {
            streamReader = spark.readStream().format(format);
            CommonUtils.setDataStreamReaderOptions(config, streamReader);
        } else {
            reader = spark.read().format(format);
            CommonUtils.setReaderOptions(config, reader);
        }

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }
    }

}
