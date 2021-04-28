package org.student.spark.processor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.student.spark.ConfigParser;
import org.student.spark.api.BaseProcessor;
import org.student.spark.api.BaseSource;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.Constants;
import org.student.spark.common.DataFlowException;

import java.util.List;
import java.util.Set;

import static org.student.spark.common.CommonUtils.extractTables;


public class Sql extends BaseProcessor {

    public String getSql() {
        return sql;
    }

    private String sql;

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), result_table_name);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    @Override
    public void process(SparkSession spark) {

        logger.info("Executor sql statement in sql processor :{}", sql);
        spark.sql(sql).createOrReplaceTempView(result_table_name);
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

        //get it from config item
        String env = CommonUtils.getEnv(config);
        String sqlDir = StringUtils.stripEnd(
                config.getString(Constants.SQL_DIR).replace("-env",
                        "-" + env)
                , "/");
        this.result_table_name = config.getString("result_table_name");

        if (config.hasPath("sql")) {
            sql = config.getString("sql");
        } else if (config.hasPath("sqlfile")) {
            String fileName = config.getString("sqlfile");
            fileName = fileName.endsWith(".sql") ? fileName : fileName + ".sql";
            sqlDir = sqlDir.isEmpty() ? "" : sqlDir.endsWith("/") ? sqlDir : sqlDir + "/";
            this.sql = CommonUtils.loadFile(sqlDir + fileName);
        } else {
            logger.error("you must specify use sql or sql file for sql processor");
            throw new DataFlowException("you must specify use sql or sql file for sql processor");
        }

        String VARIABLES = "variables";
        if (config.hasPath(VARIABLES)) {
            for (Config c : config.getConfigList(VARIABLES)) {
                this.sql = this.sql.replaceAll(c.getString("key"), c.getString("value"));
            }
        }

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }

    }
}
