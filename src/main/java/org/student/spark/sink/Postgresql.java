package org.student.spark.sink;


import org.student.spark.api.BaseSink;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.Constants;
import org.student.spark.common.JdbcUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;


public class Postgresql extends BaseSink {

    private String tag;
    private String result_table_name;
    private String numPartitions = "1";
    private String checkpointLocation;
    private JdbcOptionsInWrite options;
    private String triggerType;
    private String sql;
    private String type;
    private boolean isStreaming;
    StreamingQuery query = null;

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

        this.type = CommonUtils.getStringWithDefault(config, "type", "default");
        this.tag = CommonUtils.getStringWithDefault(config, "tag", tag) + CommonUtils.getEnv(config);
        String url = config.getString(Constants.JDBC_PREFIX + tag + ".url");
        this.table = config.getString("table");
        this.result_table_name = config.getString("result_table_name");
        String batchsize = CommonUtils.getStringWithDefault(config, "batchsize", "1000");
        this.numPartitions = CommonUtils.getStringWithDefault(config, "numPartitions", numPartitions);

        String defaultkeyPrefix = "defaults.sink.postgresql." + type + ".";
        String defaultSql = CommonUtils.getStringWithDefault(config, defaultkeyPrefix + "sql", "");
        this.sql = CommonUtils.getStringWithDefault(config, "sql", defaultSql);

        String driver = config.getString("app.jdbc." + tag + ".driver");
        scala.collection.immutable.Map<String, String> dataMap = new scala.collection.immutable.HashMap<>();
        dataMap = dataMap.$plus(new scala.Tuple2<>("url", url));
        dataMap = dataMap.$plus(new scala.Tuple2<>("driver", driver));
        dataMap = dataMap.$plus(new scala.Tuple2<>("dbtable", this.result_table_name));
        dataMap = dataMap.$plus(new scala.Tuple2<>("batchSize", String.valueOf(batchsize)));
        dataMap = dataMap.$plus(new scala.Tuple2<>("numPartitions", numPartitions));
        this.options = new JdbcOptionsInWrite(dataMap);


        isStreaming = CommonUtils.getBooleanWithDefault(config, "isStreaming", false);
        if (isStreaming) {
            prepareForStreaming();
        }
        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }
    }

    @Override
    public void process(SparkSession spark) {
        if (!spark.catalog().tableExists(table)) {
            logger.warn("No data to do for table {} in this batch", table);
            return;
        }
        Dataset<Row> df = spark.table(table);
        if (isStreaming) {
            processStreaming(spark, df);
        } else {
            JdbcUtils.upsertTable(config, tag, options, df, sql);
            if (config.hasPath("metaSource")) {
                String metaSource = config.getString("metaSource");
                String topic = config.getString("topic");
                String defaultkeyPrefix = "defaults.sink.postgresql." + type + ".";
                String defaultSql = CommonUtils.getStringWithDefault(config, defaultkeyPrefix + "postsql", "");

                String postSql = CommonUtils.getStringWithDefault(config, "postsql", defaultSql);
                String metaTag = config.getString("metaTag") + CommonUtils.getEnv(config);
                try {
                    String version = spark.table(metaSource).filter("topic='" + topic + "'")
                            .select("version").as(Encoders.STRING()).first();
                    JdbcUtils.executeSql(config, metaTag, postSql.replace("${version}", version).replace("${topic}", topic));
                } catch (Exception ex) {
                    logger.error(ex.getMessage());
                }
            }
        }
    }

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), CommonUtils.normalizeName(result_table_name));
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    private void prepareForStreaming() {
        this.save_mode = CommonUtils.getStringWithDefault(config, "savemode", "append");
        this.checkpointLocation = config.getString("checkpointLocation");
        this.triggerType = CommonUtils.getStringWithDefault(config, "trigger_type", "default");
    }

    private void processStreaming(SparkSession spark, Dataset<Row> df) {
        if (config.hasPath("scheduler.pool")) {
            spark.sparkContext().setLocalProperty("spark.scheduler.pool", config.getString("scheduler.pool"));
        }

        DataStreamWriter<Row> writer = df.writeStream().outputMode(save_mode);
        writer = writer.option("checkpointLocation", checkpointLocation);
        CommonUtils.setDataStreamTrigger(writer, config, triggerType);
        writer.queryName(table);
        try {
           query =  writer.foreachBatch((Dataset<Row> ds, Long batchId) -> {
                JdbcUtils.upsertTable(config, tag, options, ds, sql);
            }).start();
        } catch (Exception ex) {
            logger.error("error when insert into postgresql {} ", this.result_table_name);
            logger.error("error", ex);
        }
    }
}