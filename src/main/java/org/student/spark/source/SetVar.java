package org.student.spark.source;

import org.student.spark.api.BaseSource;
import org.student.spark.common.ClassUtils;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.JdbcUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * added at 2020/11/14 by yaoxhua
 * set{
 * type=[sql,jdbc,java]
 * sql=""" select cout(*) as test from test"""
 * tag=test
 * code=""" public String getName(){return \"yaoxhua\";} """
 * result_table_name=set_var
 * }
 * will be converted to ：
 * spark.sql("set test=result");
 */
public class SetVar extends BaseSource {


    private String type = "java";
    private String sql = "";
    private String code = "";
    private String tag = "";

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), result_table_name);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    @Override
    public void process(SparkSession spark) {
        Map<String, Object> result = null;
        if (type.equalsIgnoreCase("java")) {
            result = ClassUtils.executeJavaFunc(code);
        } else if (type.equalsIgnoreCase("sql")) {
            result = CommonUtils.row2JavaMap(spark.sql(sql).first());
        } else if (type.equalsIgnoreCase("jdbc")) {
            result = JdbcUtils.query(config, tag, sql).get(0);
        }
        result.forEach((k, v) -> spark.conf().set(k, v.toString()));
        CommonUtils.map2Dataframe(result, spark).createOrReplaceTempView(result_table_name);
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

        result_table_name = config.getString("result_table_name");
        type = CommonUtils.getStringWithDefault(config, "type", "sql");
        if (type.equalsIgnoreCase("java")) {
            code = config.getString("code");
        } else {
            sql = config.getString("sql");
        }
        tag = CommonUtils.getStringWithDefault(config, "tag", "test") + CommonUtils.getEnv(config);;
        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }
    }
}
