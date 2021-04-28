
package org.student.spark.sink;


import org.student.spark.api.BaseSink;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.Constants;
import org.student.spark.common.JdbcUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;


public class Jdbc extends BaseSink {

    private String user;
    private String password;
    private String tag;
    private String url;
    private String batchsize;
    private String result_table_name;
    private int numPartitions = 1;
    private boolean executeSql = false;
    private String sql ;

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

        this.executeSql = CommonUtils.getBooleanWithDefault(config, "executeSql", false);

        this.tag = CommonUtils.getStringWithDefault(config, "tag", tag) + CommonUtils.getEnv(config);
        if(this.executeSql){
            this.sql = config.getString("sql");
        } else {
            this.url = config.getString(Constants.JDBC_PREFIX + tag + ".url");
            this.user = config.getString(Constants.JDBC_PREFIX + tag + ".username");
            this.password = config.getString(Constants.JDBC_PREFIX + tag + ".password");
            this.table = config.getString("table");
            this.result_table_name = config.getString("result_table_name");
            this.batchsize = CommonUtils.getStringWithDefault(config, "batchsize", "1000");
            this.numPartitions = CommonUtils.getIntWithDefault(config, "numPartitions", numPartitions);
            this.save_mode = CommonUtils.getStringWithDefault(config, "savemode", "error");
        }

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }
    }

    @Override
    public void process(SparkSession spark) {
        if(this.executeSql) {
            JdbcUtils.executeSql(config, tag, sql);
        }
        else {
            Dataset<Row> df = spark.table(table);
            processBatch(df);
        }
    }

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), CommonUtils.normalizeName(result_table_name));
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    private void processBatch(Dataset<Row> df) {

        Properties prop = new java.util.Properties();
        prop.put("user", this.user);
        prop.put("password", this.password);
        prop.put("batchsize", this.batchsize);
        prop.put("numPartitions", String.valueOf(this.numPartitions));
        df.write().mode(save_mode).jdbc(url, result_table_name, prop);

    }

}