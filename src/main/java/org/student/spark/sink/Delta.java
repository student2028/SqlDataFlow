package org.student.spark.sink;


import org.student.spark.api.BaseSink;
import org.student.spark.common.CommonUtils;
import com.typesafe.config.Config;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.sources.DeltaDataSource;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import scala.collection.Seq;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class Delta extends BaseSink {

    private String result_table_name;
    private String checkpointLocation;
    private String triggerType;
    private String sql;
    private boolean isStreaming;
    private String path;
    private String partitionBy;
    private String conditions;
    private boolean isQuoted;
    private boolean loopPartitions = false;
    private String postSql;
    private String primaryKeys = "";
    private DataStreamWriter<Row> streamWriter = null;
    private boolean enabledCompact = true;
    StreamingQuery query = null;
    private boolean isAppendOnly = false;
    private int max_partition_files;
    private int min_partition_files;

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
        this.table = config.getString("table");
        this.save_mode = CommonUtils.getStringWithDefault(config, "savemode", "append");
        this.result_table_name = CommonUtils.getStringWithDefault(config, "result_table_name", "newData");
        //  this.numPartitions = CommonUtils.getStringWithDefault(config, "numPartitions", numPartitions);
        this.sql = CommonUtils.getStringWithDefault(config, "sql", "");
        isStreaming = CommonUtils.getBooleanWithDefault(config, "isStreaming", false);
        this.partitionBy = CommonUtils.getStringWithDefault(config, "partitionBy", "");
        this.isQuoted = CommonUtils.getBooleanWithDefault(config, "isQuoted", true);
        this.loopPartitions = CommonUtils.getBooleanWithDefault(config, "loopPartitions", false);
        this.postSql = CommonUtils.getStringWithDefault(config, "postSql", "");
        this.primaryKeys = CommonUtils.getStringWithDefault(config, "primaryKeys", "");
        this.enabledCompact = CommonUtils.getBooleanWithDefault(config, "delta.enabledCompact", true);
        this.isAppendOnly = CommonUtils.getBooleanWithDefault(config, "isAppendOnly", false);
        this.path = CommonUtils.getStringWithDefault(config, "path", "");
        this.max_partition_files = CommonUtils.getIntWithDefault(config, "max_partition_files", 50);
        this.min_partition_files = CommonUtils.getIntWithDefault(config, "min_partition_files", 10);

        if (CommonUtils.getBooleanWithDefault(config, "app.debug", false)) {
            CommonUtils.printAllFields(this);
        }
    }

    @Override
    public void process(SparkSession spark) {
        Dataset<Row> df = spark.table(table);
        if (isStreaming) {
            prepareForStreaming(spark, df);
            if (isAppendOnly) {
                appendOnlyStreaming(spark, df);
            } else {
                //if pks is not empty, use dsl not use sql
                if (primaryKeys.isEmpty()) {
                    processStreaming();
                } else {
                    processStreamingWithPK();
                }
            }
        } else {//batch
            if (sql.isEmpty()) {
                if (partitionBy.isEmpty()) {
                    df.write().format("delta").mode(save_mode).option("path", path).saveAsTable(result_table_name);
                } else {
                    df.write().format("delta").mode(save_mode).option("path", path)
                            .partitionBy(partitionBy.split(",")).saveAsTable(result_table_name);
                }
            } else {
                spark.sql(sql);
            }
        }
    }

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), CommonUtils.normalizeName(result_table_name));
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }

    private void prepareForStreaming(SparkSession spark, Dataset<Row> df) {
        this.checkpointLocation = config.getString("checkpointLocation");
        this.triggerType = CommonUtils.getStringWithDefault(config, "trigger_type", "default");
        if (config.hasPath("scheduler.pool")) {
            spark.sparkContext().setLocalProperty("spark.scheduler.pool", config.getString("scheduler.pool"));
        }
        streamWriter = df.writeStream().outputMode(save_mode);
        streamWriter.option("checkpointLocation", checkpointLocation);
        CommonUtils.setDataStreamTrigger(streamWriter, config, triggerType);
        CommonUtils.setDataStreamWriterOptions(config, streamWriter);
        streamWriter.queryName(table);
    }

    private void appendOnlyStreaming(SparkSession spark, Dataset<Row> df) {
        try {
            if (null == path || path.isEmpty()) {
                path = DeltaDataSource.extractDeltaPath(spark.table(result_table_name)).get();
            }
            logger.info(String.format("append only delta table %s's path:%s", result_table_name, path));
            if (partitionBy.isEmpty()) {
                query = streamWriter.format("delta").outputMode("append").option("path", path).start();
            } else {
                query = streamWriter.format("delta").outputMode("append").option("path", path)
                        .partitionBy(partitionBy.split(",")).start();
            }
        } catch (Exception ex) {
            logger.error("error when append to delta {} ", this.result_table_name);
            logger.error("error", ex);
            try {
                query.stop();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    private void processStreaming() {
        try {
            query = streamWriter.foreachBatch((Dataset<Row> ds, Long batchId) -> {
                //add delta_dt for each batch start date time
                String utcNow = CommonUtils.getUTCNow();
                ds.sparkSession().conf().set("__delta_ts__", utcNow);
                //prepare the partition info if partitionBy is not empty
                ds.createOrReplaceTempView(result_table_name);
                if (!partitionBy.isEmpty()) {
                    ds = ds.repartition(col(partitionBy));
                    getConditions(ds);
                    logger.info("__CONDS__:" + conditions);
                    //execute the sql one by one partition
                    if (this.loopPartitions) {
                        String[] condsArray = conditions.split(",");
                        for (String cond : condsArray) {
                            ds.sparkSession().conf().set("__conds__", cond);
                            ds.sparkSession().sql(sql);
                        }
                    } else {
                        ds.sparkSession().conf().set("__conds__", conditions);
                        ds.sparkSession().sql(sql);
                    }
                } else {
                    ds.sparkSession().sql(sql);
                }
                //do post sql
                if (!postSql.isEmpty()) {
                    ds.sparkSession().sql(postSql);
                }
                //do compact file here
                checkAndDoCompact(ds.sparkSession(),this.path);
            }).start();
        } catch (Exception ex) {
            logger.error("error when merge to delta {} ", this.result_table_name);
            logger.error("error", ex);
            try {
                query.stop();
                //not direct throw exception, but stop it here, because we want to use it in multiple query stream query case
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    private void processStreamingWithPK() {
        final String conditionFinal = getMergeCondition(primaryKeys);
        try {
            query = streamWriter.foreachBatch((Dataset<Row> ds, Long batchId) -> {
                //add delta_dt for each batch start date time
                String utcNow = CommonUtils.getUTCNow();
                Dataset<Row> shrinkedDS = ds
                        .select(ds.col("*"),
                                functions.row_number().over(Window.partitionBy(primaryKeys)
                                        .orderBy(functions.desc("V_TIMESTAMP"), functions.desc("V_OFFSET")))
                                        .alias("__rn__"))
                        .filter("__rn__=1")
                        .drop("__rn__")
                        .withColumn("COS_MOD_DATE_UTC", lit(utcNow));
                //todo :write to append only delta table here？？？？？
                DeltaTable deltaTable = DeltaTable.forName(result_table_name);
                if (partitionBy.isEmpty()) {
                    // Transform and write batchDF
                    deltaTable.as("t")
                            .merge(shrinkedDS.as("s"), conditionFinal)
                            .whenMatched("s.A_ENTTYP IN ('DL','UB')").delete()
                            .whenMatched("s.A_ENTTYP NOT IN ('DL','UB')").updateAll()
                            .whenNotMatched("s.A_ENTTYP NOT IN ('DL','UB')").insertAll()
                            .execute();
                } else {
                    getConditions(shrinkedDS);
                    String[] partitionsArray = conditions.split(",");
                    if (loopPartitions) {
                        for (String partitionValue : partitionsArray) {
                            deltaTable.as("t")
                                    .merge(shrinkedDS.as("s").filter(partitionBy + "=" + partitionValue),
                                            conditionFinal + " AND t." + partitionBy + "=" + partitionValue)
                                    .whenMatched("s.A_ENTTYP IN ('DL','UB')").delete()
                                    .whenMatched("s.A_ENTTYP NOT IN ('DL','UB')").updateAll()
                                    .whenNotMatched("s.A_ENTTYP NOT IN ('DL','UB')").insertAll()
                                    .execute();
                        }
                    } else {
                        deltaTable.as("t")
                                .merge(shrinkedDS.as("s"),
                                        conditionFinal + " AND t." + partitionBy + " IN (" + this.conditions + ")")
                                .whenMatched("s.A_ENTTYP IN ('DL','UB')").delete()
                                .whenMatched("s.A_ENTTYP NOT IN ('DL','UB')").updateAll()
                                .whenNotMatched("s.A_ENTTYP NOT IN ('DL','UB')").insertAll()
                                .execute();
                    }
                }
                //do compact file here
                checkAndDoCompact(ds.sparkSession(),this.path);
            }).start();
        } catch (Exception ex) {
            logger.error("error when merge to delta {} ", this.result_table_name);
            logger.error("error", ex);
            try {
                query.stop();
                //not direct throw exception, but stop it here, because we want to use it in multiple query stream query case
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    void getConditions(Dataset<Row> ds) {
        if (isQuoted)
            conditions = "'" +
                    ds.selectExpr(String.format("concat_ws('\\',\\'',collect_set(%s))", partitionBy)).first().getString(0)
                    + "'";
        else
            conditions = ds.selectExpr(String.format("concat_ws(',',collect_set(%s))", partitionBy)).first().getString(0);
    }

    protected String getMergeCondition(String primaryKey) {
        if (primaryKey.isEmpty())
            return "";
        String and = " AND ";
        int i = 1;
        String mergeCondition = "";
        for (String col : primaryKey.replaceAll(" ", "").toUpperCase().split(",")) {
            if (i != 1) {
                mergeCondition = mergeCondition + and;
            }
            mergeCondition = mergeCondition + "TRIM(s." + col + ") = TRIM(t." + col + ")";
            i++;
        }
        return mergeCondition;
    }


    protected void checkAndDoCompact(SparkSession spark,String tablePath) {
        if (!this.enabledCompact) {
            logger.warn("It's suggested that auto-compacting small files with configuration 'delta.enabledCompact'=true");
            return;
        }

        try {
            if (this.partitionBy.isEmpty()) {
                compactSmallFilesWithNonPartitionedTable(spark, tablePath);
            } else {
                compactSmallFilesWithPartitionedTable(spark, tablePath);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    protected void compactSmallFilesWithNonPartitionedTable(SparkSession spark, String tablePath) {
        logger.info("prepare to compact small files without table partitioned");

        Seq<String> partitionSeq = DeltaLog.forTable(spark, tablePath)
                .metadata()
                .partitionColumns();
        logger.info("partition info:{}", partitionSeq);

        if (!partitionSeq.isEmpty()) {
            logger.warn("detected partition column(s), compaction canceled");
            return;
        }

        long numOfFiles = DeltaLog.forTable(spark, tablePath).snapshot().numOfFiles();

        if (numOfFiles <= this.max_partition_files) {
            logger.info("files are not over limitation, compaction canceled");
            return;
        }

        DeltaTable.forPath(spark, tablePath)
                .toDF()
                .repartition(this.min_partition_files)
                .write()
                .format("delta")
                .mode("overwrite")
                .option("dataChange", "false")
                .save(tablePath);

        logger.info("compaction done");

    }


    protected void compactSmallFilesWithPartitionedTable(SparkSession spark, String tablePath) {
        logger.info("prepare to compact small files with table partitioned");

        Seq<String> partitionSeq = DeltaLog.forTable(spark, tablePath)
                .metadata()
                .partitionColumns();
        logger.info("partition info:{}", partitionSeq);

        if (partitionSeq.isEmpty()) {
            logger.warn("not detected partition column(s), compaction canceled");
            return;
        }
        if (!partitionSeq.isEmpty() && !(partitionSeq.size() == 1)) {
            logger.warn("un-support multiple partitions(num>1), compaction canceled");
            return;
        }
        String partitionColName = partitionSeq.head();

        Dataset<AddFile> allFilesDataset = DeltaLog.forTable(spark, tablePath)
                .snapshot()
                .allFiles();
        allFilesDataset.cache();

        String partition = allFilesDataset
                .select(functions.max(functions.col("partitionValues").getItem(partitionColName)))
                .as(Encoders.STRING())
                .first();

        long numOfPartitionFile = allFilesDataset
                .filter(functions.col("partitionValues").getItem(partitionColName).equalTo(partition))
                .count();

        allFilesDataset.unpersist();

        logger.info("partition columnName:{}, value:{}, files:{}", partitionColName, partition, numOfPartitionFile);

        if (numOfPartitionFile <= this.max_partition_files) {
            logger.info("files are not over limitation, compaction canceled");
            return;
        }

        DeltaTable.forPath(spark, tablePath)
                .toDF()
                .where(functions.col(partitionColName).equalTo(partition))
                .repartition(this.min_partition_files)
                .write()
                .format("delta")
                .mode("overwrite")
                .option("dataChange", "false")
                .option("replaceWhere", String.format("%s='%s'", partitionColName, partition))
                .save(tablePath);

        logger.info("compact done");

    }
}