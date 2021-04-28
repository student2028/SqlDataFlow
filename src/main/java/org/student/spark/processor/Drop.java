package org.student.spark.processor;

import org.student.spark.api.BaseProcessor;
import org.student.spark.common.CommonUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Drop extends BaseProcessor {

     private String[] fields;

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(),table);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    @Override
    public void process(SparkSession spark) {
        Dataset<Row> df = spark.table(table);
        df.drop(fields)
                .createOrReplaceTempView(result_table_name);
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
        return false;
    }

    @Override
    public void prepare(SparkSession spark) {

        table = config.getString("table");
        result_table_name = CommonUtils.getStringWithDefault(config, "result_table_name", table);
        fields = config.getStringList("source_field").toArray(new String[0]);

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }

    }
}
