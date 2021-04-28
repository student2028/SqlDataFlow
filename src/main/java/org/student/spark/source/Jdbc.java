package org.student.spark.source;

import org.student.spark.api.BaseSource;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.JdbcUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;

public class Jdbc extends BaseSource {

    private String tag;
    private String partitionColumn;
    private String numPartitions;
    private String lowerBound;
    private String upperBound;
    private DataFrameReader reader;
    private String table;
    private String presql;//execute before execute sql ,use to refresh materialized view or others
    private boolean isCached = false;

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), result_table_name);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    @Override
    public void process(SparkSession spark) {
        //handle variable
         if (!presql.isEmpty()) {
            JdbcUtils.executeSql(config, tag, presql);
        }
        this.table = CommonUtils.replaceVariable(table,spark);
        logger.info(table);
        reader.option("dbtable",table);
        Dataset<Row> df = reader.load();
        if (isCached) {
            df.cache();
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
        //tag identified in application.conf
        tag = config.getString("tag") + CommonUtils.getEnv(config);
        table = config.getString("table");
        String url = config.getString("app.jdbc." + tag + ".url");
        String user = config.getString("app.jdbc." + tag + ".username");
        String password = config.getString("app.jdbc." + tag + ".password");
        String uuid = "jdbc_" + UUID.randomUUID().toString().replaceAll("-", "");
        result_table_name = CommonUtils.getStringWithDefault(config, "result_table_name", uuid);
        presql = CommonUtils.getStringWithDefault(config, "presql", "");
        isCached = CommonUtils.getBooleanWithDefault(config, "cached", isCached);

        reader = spark.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("user", user)
                .option("password", password);

        if (config.hasPath("partitionColumn")) {
            partitionColumn = config.getString("partitionColumn");
            lowerBound = config.getString("lowerBound");
            upperBound = config.getString("upperBound");
            reader = reader.option("partitionColumn", partitionColumn)
                    .option("lowerBound", lowerBound)
                    .option("upperBound", upperBound);
        }
        if (config.hasPath("numPartitions")) {
            numPartitions = config.getString("numPartitions");
            reader = reader.option("numPartitions", numPartitions);
        }

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }


    }

}
