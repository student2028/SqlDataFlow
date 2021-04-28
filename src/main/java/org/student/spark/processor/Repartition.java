package org.student.spark.processor;

import org.student.spark.api.BaseProcessor;
import org.student.spark.common.CommonUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Repartition extends BaseProcessor {

    private int numPartitions;
    private String partitionBy;

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), table);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    @Override
    public void process(SparkSession spark) {
        Dataset<Row> df = spark.table(table);
        int partitions = df.rdd().getNumPartitions();
        if (!partitionBy.isEmpty()) {
            df = df.repartition(col(partitionBy));
        } else if (partitions > numPartitions) {
            df = df.coalesce(numPartitions);
        } else if (partitions < numPartitions) {
            df = df.repartition(numPartitions);
        }

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
        return false;
    }

    @Override
    public void prepare(SparkSession spark) {

        table = config.getString("table");
        result_table_name = CommonUtils.getStringWithDefault(config, "result_table_name", table);
        numPartitions = CommonUtils.getIntWithDefault(config, "num_partitions", 10);
        partitionBy = CommonUtils.getStringWithDefault(config, "partitionBy", "");

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }

    }
}
