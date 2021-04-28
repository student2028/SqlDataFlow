package org.student.spark.sink;


import org.student.spark.api.BaseSink;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.Constants;
import org.student.spark.common.JdbcUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.lit;

public class DB extends BaseSink {

    private String tag;
    private String result_table_name;
    private String numPartitions = "0";
    private String checkpointLocation;
    private JdbcOptionsInWrite options;
    private String triggerType;
    private String sql;
    private String type;
    private boolean isStreaming;


    StreamingQuery query = null;
    //used for new sink
    private String softDeletetionColumnName;
    private String pkColumns;
    private String addDateColumn = "SINK_ADD_DATE";
    private String modDateColumn = "SINK_MOD_DATE";
    private LinkedHashMap<String, String> dbCols;
    private DataStreamWriter<Row> writer;

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

        String defaultkeyPrefix = "defaults.sink.db." + type + ".";
        String defaultSql = CommonUtils.getStringWithDefault(config, defaultkeyPrefix + "sql", "");
        this.sql = CommonUtils.getStringWithDefault(config, "sql", defaultSql);

        String driver = config.getString("app.jdbc." + tag + ".driver");
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("url", url);
        optionsMap.put("driver", driver);
        optionsMap.put("dbtable", this.result_table_name);
        optionsMap.put("batchSize", batchsize);
        optionsMap.put("numPartitions", numPartitions);
        this.options = new JdbcOptionsInWrite(CommonUtils.toScalaImmutableMap(optionsMap));

        //toUpperCase to ignore
        softDeletetionColumnName = CommonUtils.getStringWithDefault(config, "softDelCol", "").toUpperCase();
        pkColumns = CommonUtils.getStringWithDefault(config, "primaryKeys", "").toUpperCase();
        addDateColumn = CommonUtils.getStringWithDefault(config, "addDateColumn", addDateColumn).toUpperCase();
        modDateColumn = CommonUtils.getStringWithDefault(config, "modDateColumn", modDateColumn).toUpperCase();

        isStreaming = CommonUtils.getBooleanWithDefault(config, "isStreaming", false);

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }
    }

    @Override
    public void process(SparkSession spark) {
        if (!spark.catalog().tableExists(table)) {
             return;
        }
        Dataset<Row> df = spark.table(table);
        if (isStreaming) {
            prepareForStreaming(spark, df);
            if (pkColumns.isEmpty()) {
                processStreaming();
            } else {
                processStreamingWithPK();
            }
        } else {
            JdbcUtils.upsertTable(config, tag, options, df, sql);
        }
    }

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), CommonUtils.normalizeName(result_table_name));
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    private void prepareForStreaming(SparkSession spark, Dataset<Row> df) {
        this.save_mode = CommonUtils.getStringWithDefault(config, "savemode", "append");
        this.checkpointLocation = config.getString("checkpointLocation");
        this.triggerType = CommonUtils.getStringWithDefault(config, "trigger_type", "default");
        if (config.hasPath("scheduler.pool")) {
            spark.sparkContext().setLocalProperty("spark.scheduler.pool", config.getString("scheduler.pool"));
        }
        writer = df.writeStream().outputMode(save_mode);
        writer.option("checkpointLocation", checkpointLocation);
        CommonUtils.setDataStreamTrigger(writer, config, triggerType);
        writer.queryName(table);

    }

    private void processStreaming() {
        try {
            query = writer.foreachBatch((Dataset<Row> ds, Long batchId) -> {
                if (options.url().startsWith("jdbc:db2")) {
                    Map<String, String> map = JdbcUtils.queryTableColumns(config, tag, result_table_name);
                    ds = CommonUtils.castDataset(ds, map);
                }
                JdbcUtils.upsertTable(config, tag, options, ds, sql);
            }).start();
        } catch (Exception ex) {
            logger.error("error when insert into db {} ", this.result_table_name);
            logger.error("error", ex);
        }
    }

    private void processStreamingWithPK() {
        try {
            query = writer.foreachBatch((Dataset<Row> ds, Long batchId) -> {
                if (options.url().startsWith("jdbc:db2")) {
                    Map<String, String> map = JdbcUtils.queryTableColumns(config, tag, result_table_name);
                    ds = CommonUtils.castDataset(ds, map);
                }
                //revised sql for ds : columns and deduplicate
                //generate sql use tableName and pks and df
                Dataset<Row> newData = generateMergeSql(config, tag, ds);
                JdbcUtils.upsertTable(config, tag, options, newData, sql);
            }).start();
        } catch (Exception ex) {
            logger.error("error when insert into db {} ", this.result_table_name);
            logger.error("error", ex);
        }
    }

     protected Dataset<Row> generateMergeSql(Config config, String tag, Dataset<Row> df) {
        //todo if every batch will generate this ,it will cause performance issue
        // Either "SINK_SOFT_DLT_IND" or "DELETED_FLAG" could be the expected column name
        //if cos have,use cos value if mot use current_timestamp
        //Get all fields with their NULL attribute from DB
        //final LinkedHashMap<String, String> nameAndNulls = SinkToolkit.getColNameAndNulls(config, tag, targetTable);
        // get all column's name and their type from DB.
        if (this.sql.isEmpty()) {
            dbCols = JdbcUtils.getColNameAndType(config, tag, result_table_name.toUpperCase());
        }
        List<String> dfCols = Arrays.asList(df.columns());
        List<String> notUsedColsInDB = Arrays.stream(df.columns()).filter(col -> !dbCols.containsKey(col)
                && !col.equalsIgnoreCase("A_ENTTYP")
        ).collect(Collectors.toList());
        String utcNow = CommonUtils.getUTCNow();
        if (dbCols.containsKey(addDateColumn) &&
                !dfCols.contains(addDateColumn)) {
            df = df.withColumn(addDateColumn, lit(utcNow));
        }
        if (dbCols.containsKey(modDateColumn) &&
                !dfCols.contains(modDateColumn)) {
            df = df.withColumn(modDateColumn, lit(utcNow));
        }
        //handle cases for soft delete field exists not delete just update with del_ind=1
        if (!softDeletetionColumnName.isEmpty()) {
            df = df.withColumn(softDeletetionColumnName, functions.when(functions.col("A_ENTTYP").isin("UB", "DL"), 1).otherwise(0));
            df = df.withColumn("A_ENTTYP", functions.when(functions.col("A_ENTTYP").isin("UB", "DL"), "UP")
                    .otherwise(functions.col("A_ENTTYP")));
        }

        Dataset<Row> newDF = df
                .select(df.col("*"),
                        functions.row_number().over(Window.partitionBy(pkColumns)
                                .orderBy(functions.desc("V_TIMESTAMP"), functions.desc("V_OFFSET")))
                                .alias("__rn__"))
                .filter("__rn__=1")
                .drop("__rn__");
        //discard fields that df has but db not used
        for (String col : notUsedColsInDB) {
            newDF = newDF.drop(col);
        }
        //generate variables for sql template
        if (this.sql.isEmpty()) {
            List<String> pktList = Arrays.asList(pkColumns.split(","));
            List<String> nonPKCols = Arrays.stream(newDF.columns()).filter(c -> !pktList.contains(c)
                    && !c.equalsIgnoreCase("A_ENTTYP")
                    && !c.equalsIgnoreCase("V_OFFSET")
                    && !c.equalsIgnoreCase("V_TIMESTAMP")
            ).collect(Collectors.toList());
            String placeholders = Arrays.stream(newDF.columns()).map(x -> "?").collect(Collectors.joining(","));
            String srcCols = Arrays.stream(newDF.columns()).collect(Collectors.joining(","));
            String joinCols = "T." + pkColumns.replaceAll(" ", "").replaceAll(",", ",T.") + "=" +
                    "S." + pkColumns.replaceAll(" ", "").replaceAll(",", ",S.");
            String updateClause = nonPKCols.stream().map(c -> "T." + c).collect(Collectors.joining(",", "(", ")="))
                    + nonPKCols.stream().map(c -> "S." + c).collect(Collectors.joining(",", "(", ")"));
            String insertColsClause = Arrays.stream(newDF.columns()).filter(c -> !c.equalsIgnoreCase("A_ENTTYP")).collect(Collectors.joining(","));
            String insertValColsClause = "S." + insertColsClause.replaceAll(",", ",S.");
            this.sql = String.format(Constants.MERGE_TO_DASH_STMT, result_table_name.toUpperCase(), placeholders, srcCols, joinCols, updateClause, insertColsClause, insertValColsClause);
        }

        return newDF;
    }

}