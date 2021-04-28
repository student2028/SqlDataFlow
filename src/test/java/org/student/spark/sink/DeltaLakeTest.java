package org.student.spark.sink;

import org.student.spark.SparkTestBase;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class DeltaLakeTest extends SparkTestBase {

    @Test
    public void testDeltaWithPK() throws StreamingQueryException, IOException {
        //create two delta tables sdf_delta1 is source table, sdf_delta1_x is target table
        //we will use streaming read data from source to target then merge data
        spark.sql("DROP TABLE IF EXISTS sdf_delta1");
        spark.sql("DROP TABLE IF EXISTS sdf_delta1_x");
        //    FileUtils.deleteDirectory(new java.io.File("spark-warehouse"));
        spark.sql("CREATE TABLE sdf_delta1(id long, name string, A_ENTTYP string, V_TIMESTAMP timestamp, V_OFFSET long) using delta");
        spark.sql("CREATE TABLE sdf_delta1_x(id long, name string,COS_MOD_DATE_UTC timestamp, A_ENTTYP string, V_TIMESTAMP timestamp, V_OFFSET long) using delta");
        Dataset<Row> df = spark.range(1, 5).selectExpr("id", "cast(id as string) as name", "'PT' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(id+1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        df = spark.range(1, 2).selectExpr("id", "'1' as name", "'DL' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        df = spark.range(2, 3).selectExpr("id", "'22' as name", "'UP' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        //data prepare finished so the sdf_delta1_x should have 3 rows 2 3 4 AND 222 IS NAME OF ID=2
        spark.readStream().format("delta").load("spark-warehouse/sdf_delta1").createOrReplaceTempView("sdf_delta1_file");
        String checkpointDir = System.getProperty("java.io.tmpdir") + "checkpoints/sdf_delta1";
        java.io.File checkpointFile = new java.io.File(checkpointDir);
        if (checkpointFile.exists()) FileUtils.deleteDirectory(checkpointFile);

        String json = "{\n" +
                "         checkpointLocation=\"" + checkpointDir + "\"\n" +
                "         isStreaming=true\n" +
                "         trigger_type=ProcessingTime\n" +
                "         interval=\"1 seconds\"\n" +
                "         table=sdf_delta1_file\n" +
                "         result_table_name=\"sdf_delta1_x\"\n" +
                "         primaryKeys=\"id\"\n" +
                "     }";
        Config config = ConfigFactory.parseString(json);
        Delta delta1 = new Delta();
        delta1.setConfig(config);
        delta1.prepare(spark);
        delta1.process(spark);
        delta1.query.awaitTermination(10000);
        int count = spark.sql("select cast(count(1) as int) as cnt from sdf_delta1_x").as(Encoders.INT()).first();
        String name = spark.sql("select name from sdf_delta1_x where id=2").as(Encoders.STRING()).first();
//        +---+----+-----------------------+--------+-----------------------+--------+
//        |id |name|COS_MOD_DATE_UTC       |A_ENTTYP|V_TIMESTAMP            |V_OFFSET|
//        +---+----+-----------------------+--------+-----------------------+--------+
//        |2  |22  |2021-02-08 15:13:28.381|UP      |2021-02-08 23:13:25.777|1       |
//        |4  |4   |2021-02-08 15:13:28.381|PT      |2021-02-08 23:13:21.731|5       |
//        |3  |3   |2021-02-08 15:13:28.381|PT      |2021-02-08 23:13:21.731|4       |
//        +---+----+-----------------------+--------+-----------------------+--------+
        assertEquals(3, count);
        assertEquals("22", name);
        spark.sql("select * from sdf_delta1_x").show(10, false);
        spark.sql("drop table if exists sdf_delta1");
        spark.sql("drop table if exists sdf_delta1_x");

    }


    @Test
    public void testDeltaWithPKAndPartition() throws StreamingQueryException, IOException {
        //partition by month of v_timestamp DATE_FORMAT(CURRENT_TIMESTAMP,'yyyyMM') AS DT
        spark.sql("DROP TABLE IF EXISTS sdf_delta1");
        spark.sql("DROP TABLE IF EXISTS sdf_delta1_px");
        //  FileUtils.deleteDirectory(new java.io.File("spark-warehouse"));
        spark.sql("CREATE TABLE sdf_delta1(id long, name string, A_ENTTYP string, V_TIMESTAMP timestamp, V_OFFSET long) using delta");
        spark.sql("CREATE TABLE sdf_delta1_px(id long, name string,COS_MOD_DATE_UTC timestamp, A_ENTTYP string, V_TIMESTAMP timestamp, V_OFFSET long,DT STRING) USING DELTA PARTITIONED BY (DT)");
        Dataset<Row> df = spark.range(1, 5).selectExpr("id", "cast(id as string) as name", "'PT' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(id+1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        df = spark.range(1, 2).selectExpr("id", "'1' as name", "'DL' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        df = spark.range(2, 3).selectExpr("id", "'22' as name", "'UP' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        //data prepare finished so the sdf_delta1_x should have 3 rows 2 3 4 AND 222 IS NAME OF ID=2
        spark.readStream().format("delta").load("spark-warehouse/sdf_delta1").createOrReplaceTempView("sdf_delta1_file");
        spark.table("sdf_delta1_file").selectExpr("*", "DATE_FORMAT(CURRENT_TIMESTAMP,'yyyyMM') AS DT").createOrReplaceTempView("sdf_delta1_sql");
        String checkpointDir = System.getProperty("java.io.tmpdir") + "checkpoints/sdf_delta1";
        java.io.File checkpointFile = new java.io.File(checkpointDir);
        if (checkpointFile.exists()) FileUtils.deleteDirectory(checkpointFile);

        String json = "{\n" +
                "         checkpointLocation=\"" + checkpointDir + "\"\n" +
                "         isStreaming=true\n" +
                "         trigger_type=ProcessingTime\n" +
                "         interval=\"1 seconds\"\n" +
                "         table=sdf_delta1_sql\n" +
                "         result_table_name=\"sdf_delta1_px\"\n" +
                "         primaryKeys=\"id\"\n" +
                "         partitionBy=DT \n" +
                "     }";
        Config config = ConfigFactory.parseString(json);
        Delta delta1 = new Delta();
        delta1.setConfig(config);
        delta1.prepare(spark);
        delta1.process(spark);
        delta1.query.awaitTermination(10000);
        int count = spark.sql("select cast(count(1) as int) as cnt from sdf_delta1_px").as(Encoders.INT()).first();
        String name = spark.sql("select name from sdf_delta1_px where id=2").as(Encoders.STRING()).first();
        spark.sql("select * from sdf_delta1_px").show(10, false);
        /*
        * +---+----+-----------------------+--------+-----------------------+--------+------+
        |id |name|COS_MOD_DATE_UTC       |A_ENTTYP|V_TIMESTAMP            |V_OFFSET|DT    |
        +---+----+-----------------------+--------+-----------------------+--------+------+
        |2  |22  |2021-02-09 00:25:24.015|UP      |2021-02-09 08:25:20.145|1       |202102|
        |3  |3   |2021-02-09 00:25:24.015|PT      |2021-02-09 08:25:16.438|4       |202102|
        |4  |4   |2021-02-09 00:25:24.015|PT      |2021-02-09 08:25:16.438|5       |202102|
        +---+----+-----------------------+--------+-----------------------+--------+------+
        * */
        assertEquals(3, count);
        assertEquals("22", name);
        spark.sql("drop table if exists sdf_delta1");
        spark.sql("drop table if exists sdf_delta1_px");
    }

    @Test
    public void testDeltaWithPKAndLooopPartition() throws StreamingQueryException, IOException {
        //partition by month of v_timestamp DATE_FORMAT(CURRENT_TIMESTAMP,'yyyyMM') AS DT
        spark.sql("DROP TABLE IF EXISTS sdf_delta1");
        spark.sql("DROP TABLE IF EXISTS sdf_delta1_px");
        FileUtils.deleteDirectory(new java.io.File("spark-warehouse"));
        spark.sql("CREATE TABLE sdf_delta1(id long, name string, A_ENTTYP string, V_TIMESTAMP timestamp, V_OFFSET long) using delta");
        spark.sql("CREATE TABLE sdf_delta1_px(id long, name string,COS_MOD_DATE_UTC timestamp, A_ENTTYP string, V_TIMESTAMP timestamp, V_OFFSET long,DT STRING) USING DELTA PARTITIONED BY (DT)");
        Dataset<Row> df = spark.range(1, 5).selectExpr("id", "cast(id as string) as name", "'PT' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(id+1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        df = spark.range(1, 2).selectExpr("id", "'1' as name", "'DL' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        df = spark.range(2, 3).selectExpr("id", "'22' as name", "'UP' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        //data prepare finished so the sdf_delta1_x should have 3 rows 2 3 4 AND 222 IS NAME OF ID=2
        spark.readStream().format("delta").load("spark-warehouse/sdf_delta1").createOrReplaceTempView("sdf_delta1_file");
        spark.table("sdf_delta1_file").selectExpr("*", "DATE_FORMAT(CURRENT_TIMESTAMP,'yyyyMM') AS DT").createOrReplaceTempView("sdf_delta1_sql2");
        String checkpointDir = System.getProperty("java.io.tmpdir") + "checkpoints/sdf_delta1";
        java.io.File checkpointFile = new java.io.File(checkpointDir);
        if (checkpointFile.exists()) FileUtils.deleteDirectory(checkpointFile);

        String json = "{\n" +
                "         checkpointLocation=\"" + checkpointDir + "\"\n" +
                "         isStreaming=true\n" +
                "         trigger_type=ProcessingTime\n" +
                "         interval=\"1 seconds\"\n" +
                "         table=sdf_delta1_sql2\n" +
                "         result_table_name=\"sdf_delta1_px\"\n" +
                "         primaryKeys=\"id\"\n" +
                "         partitionBy=DT \n" +
                "         loopPartitions=true" +
                "     }";
        Config config = ConfigFactory.parseString(json);
        Delta delta1 = new Delta();
        delta1.setConfig(config);
        delta1.prepare(spark);
        delta1.process(spark);
        delta1.query.awaitTermination(10000);
        int count = spark.sql("select cast(count(1) as int) as cnt from sdf_delta1_px").as(Encoders.INT()).first();
        String name = spark.sql("select name from sdf_delta1_px where id=2").as(Encoders.STRING()).first();
        spark.sql("select * from sdf_delta1_px").show(10, false);
        /*
        * +---+----+-----------------------+--------+-----------------------+--------+------+
        |id |name|COS_MOD_DATE_UTC       |A_ENTTYP|V_TIMESTAMP            |V_OFFSET|DT    |
        +---+----+-----------------------+--------+-----------------------+--------+------+
        |2  |22  |2021-02-09 00:25:24.015|UP      |2021-02-09 08:25:20.145|1       |202102|
        |3  |3   |2021-02-09 00:25:24.015|PT      |2021-02-09 08:25:16.438|4       |202102|
        |4  |4   |2021-02-09 00:25:24.015|PT      |2021-02-09 08:25:16.438|5       |202102|
        +---+----+-----------------------+--------+-----------------------+--------+------+
        * */
        assertEquals(3, count);
        assertEquals("22", name);
        spark.sql("drop table if exists sdf_delta1");
        spark.sql("drop table if exists sdf_delta1_px");
    }

    @Test
    public void testDeltaSinkWithSql() throws StreamingQueryException, IOException {
        //FileUtils.deleteDirectory(new java.io.File("spark-warehouse"));
        spark.sql("DROP TABLE IF EXISTS test_delta");
        spark.sql("CREATE TABLE IF NOT EXISTS test_delta (id long, name string) using delta");
        makeStreamSource("8888", "8888", "test8888");
        Delta delta1 = new Delta();
        String json = "{ scheduler.pool=pool8888\n" +
                "        checkpointLocation=\"/tmp/sdf/testdelta/\"\n" +
                "        isStreaming=true\n" +
                "        table=test8888\n" +
                "        result_table_name=newData\n" +
                "        sql=\"\"\"\n" +
                "            MERGE INTO test_delta as t USING ( SELECT id , name  FROM (SELECT id,name, " +
                "            ROW_NUMBER() OVER(PARTITION BY ID ORDER BY id)  as __rn__ FROM newdata ) as a WHERE a.__rn__=1)as s " +
                "            ON t.id=s.id\n" +
                "            WHEN MATCHED THEN\n" +
                "            UPDATE SET *\n" +
                "            WHEN NOT MATCHED THEN INSERT *\n" +
                "       \"\"\"\n" +
                "    }";

        Config config = ConfigFactory.parseString(json).withFallback(configParser.getConfig());

        delta1.setConfig(config);
        delta1.getPluginName();
        delta1.prepare(spark);
        delta1.process(spark);
        delta1.query.awaitTermination(5000);
        int count = spark.sql("SELECT CAST(count(1) as int) as cnt FROM test_delta WHERE name='8888'").as(Encoders.INT()).first();
        spark.sql("DROP TABLE IF EXISTS test_delta");
        assertEquals(1, count);
    }

    @Test
    public void testDeltaSinkWithSqlAndPartition() throws StreamingQueryException, IOException {
        // FileUtils.deleteDirectory(new java.io.File("spark-warehouse"));
        spark.sql("DROP TABLE IF EXISTS test_delta_sql_p");
        spark.sql("CREATE TABLE IF NOT EXISTS test_delta_sql_p (id long, name string, DT STRING) USING DELTA PARTITIONED BY (DT)");
        makeStreamSource("10000", "00000", "test00000");
        String checkpointDir = getCheckpointDir("test00000");
        spark.table("test00000").selectExpr("*", "DATE_FORMAT(CURRENT_TIMESTAMP,'yyyyMM') AS DT").createOrReplaceTempView("test00000_sql");
        Delta delta1 = new Delta();
        String json = "{ scheduler.pool=pool00000\n" +
                "        checkpointLocation=\"" + checkpointDir + "\"\n" +
                "        isStreaming=true\n" +
                "        table=test00000_sql\n" +
                "        partitionBy=DT \n" +
                "        result_table_name=newData\n" +
                "        sql=\"\"\"\n" +
                "            MERGE INTO test_delta_sql_p AS t USING (SELECT id , name, dt  FROM (SELECT id,name, dt," +
                "            ROW_NUMBER() OVER(PARTITION BY ID ORDER BY id)  AS __rn__ FROM newdata ) AS a WHERE a.__rn__=1) AS s " +
                "            ON t.id=s.id AND t.dt IN (${__conds__})\n" +
                "            WHEN MATCHED THEN\n" +
                "            UPDATE SET *\n" +
                "            WHEN NOT MATCHED THEN INSERT *\n" +
                "       \"\"\"\n" +
                "    }";

        Config config = ConfigFactory.parseString(json).withFallback(configParser.getConfig());
        delta1.setConfig(config);
        delta1.prepare(spark);
        delta1.process(spark);
        delta1.query.awaitTermination(20000);
        int count = spark.sql("SELECT CAST(count(1) AS INT) AS CNT FROM test_delta_sql_p").as(Encoders.INT()).first();
        spark.sql("SELECT * FROM test_delta_sql_p").show(100, false);
        //  spark.sql("DROP TABLE IF EXISTS test_delta_sql_p");
        assertEquals(1, count);
    }

    @Test
    public void testDeltaSinkWithSqlAndLoopPartition() throws StreamingQueryException, IOException, InterruptedException {
        // FileUtils.deleteDirectory(new java.io.File("spark-warehouse"));
        spark.sql("DROP TABLE IF EXISTS test_delta_sql_p2");
        spark.sql("CREATE TABLE IF NOT EXISTS test_delta_sql_p2 (id long, name string, DT STRING) USING DELTA PARTITIONED BY (DT)");
        makeStreamSource("20000", "20000", "test20000");
        String checkpointDir = getCheckpointDir("test20000");
        spark.table("test20000").selectExpr("*", "DATE_FORMAT(CURRENT_TIMESTAMP,'yyyyMM') AS DT").createOrReplaceTempView("test20000_sql");
        Delta delta1 = new Delta();
        String json = "{ scheduler.pool=pool00001\n" +
                "        checkpointLocation=\"" + checkpointDir + "\"\n" +
                "        isStreaming=true\n" +
                "        table=test20000_sql\n" +
                "        partitionBy=DT \n" +
                "        loopPartitions=true\n" +
                "        result_table_name=newData\n" +
                "        sql=\"\"\"\n" +
                "            MERGE INTO test_delta_sql_p2 AS t USING (SELECT id , name, dt  FROM (SELECT id,name, dt," +
                "            ROW_NUMBER() OVER(PARTITION BY ID ORDER BY id)  AS __rn__ FROM newdata ) AS a WHERE a.__rn__=1) AS s " +
                "            ON t.id=s.id AND t.dt IN (${__conds__})\n" +
                "            WHEN MATCHED THEN\n" +
                "            UPDATE SET *\n" +
                "            WHEN NOT MATCHED THEN INSERT *\n" +
                "       \"\"\"\n" +
                "    }";

        Config config = ConfigFactory.parseString(json).withFallback(configParser.getConfig());
        delta1.setConfig(config);
        delta1.prepare(spark);
        delta1.process(spark);
        delta1.query.awaitTermination(20000);
        int count = spark.sql("SELECT CAST(count(1) AS INT) AS CNT FROM test_delta_sql_p2").as(Encoders.INT()).first();
        spark.sql("SELECT * FROM test_delta_sql_p2").show(100, false);
        assertEquals(1, count);
    }
    //test write data to delta batch

    @Test
    public void testDeltaBatch() throws IOException {
        spark.sql("DROP TABLE IF EXISTS sdf_delta1");
        spark.sql("DROP TABLE IF EXISTS sdf_delta1_batch");
        spark.sql("CREATE TABLE sdf_delta1(id long, name string, A_ENTTYP string, V_TIMESTAMP timestamp, V_OFFSET long) using delta");
        Dataset<Row> df = spark.range(1, 5).selectExpr("id", "cast(id as string) as name", "'PT' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(id+1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        df = spark.range(1, 2).selectExpr("id", "'1' as name", "'DL' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        df = spark.range(2, 3).selectExpr("id", "'22' as name", "'UP' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        spark.read().format("delta").load("spark-warehouse/sdf_delta1").createOrReplaceTempView("sdf_delta1_file");
        String json = "{\n" +
                "         isStreaming=false\n" +
                "         table=sdf_delta1_file\n" +
                "         result_table_name=\"sdf_delta1_batch\"\n" +
                "         path=\"spark-warehouse/sdf_delta1_batch/\"" +
                "     }";
        Config config = ConfigFactory.parseString(json);
        Delta delta1 = new Delta();
        delta1.setConfig(config);
        delta1.prepare(spark);
        delta1.process(spark);
        int count = spark.sql("select cast(count(1) as int) as cnt from sdf_delta1_batch").as(Encoders.INT()).first();
        assertEquals(6, count);
        spark.sql("select * from sdf_delta1_batch").show(10, false);
        spark.sql("drop table if exists sdf_delta1_batch");

    }

    @Test
    public void testDeltaPartitionBatch() throws IOException {
        spark.sql("DROP TABLE IF EXISTS sdf_delta1");
        spark.sql("DROP TABLE IF EXISTS sdf_delta1_batch2");
        spark.sql("CREATE TABLE sdf_delta1(id long, name string, A_ENTTYP string, V_TIMESTAMP timestamp, V_OFFSET long) USING DELTA");
        Dataset<Row> df = spark.range(1, 5).selectExpr("id", "cast(id as string) as name", "'PT' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(id+1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        df = spark.range(1, 2).selectExpr("id", "'1' as name", "'DL' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        df = spark.range(2, 3).selectExpr("id", "'22' as name", "'UP' AS A_ENTTYP", "CURRENT_TIMESTAMP AS V_TIMESTAMP", "CAST(1 AS LONG)AS V_OFFSET");
        df.write().format("delta").mode("append").saveAsTable("sdf_delta1");
        spark.read().format("delta").load("spark-warehouse/sdf_delta1")
                .selectExpr("*", "DATE_FORMAT(CURRENT_TIMESTAMP,'yyyyMM') AS DT").createOrReplaceTempView("test20001_sql");

        String json = "{\n" +
                "         isStreaming=false\n" +
                "         table=test20001_sql\n" +
                "         result_table_name=\"sdf_delta1_batch2\"\n" +
                "         path=\"spark-warehouse/sdf_delta1_batch2/\"\n" +
                "         partitionBy=DT \n" +
                "     }";
        Config config = ConfigFactory.parseString(json);
        Delta delta1 = new Delta();
        delta1.setConfig(config);
        delta1.prepare(spark);
        delta1.process(spark);
        int count = spark.sql("select cast(count(1) as int) as cnt from sdf_delta1_batch2").as(Encoders.INT()).first();
        assertEquals(6, count);
        spark.sql("select * from sdf_delta1_batch2").show(10, false);
        spark.sql("drop table if exists sdf_delta1_batch2");

    }

    @Test
    public void testDeltaLakeAppendOnlyStreaming() throws StreamingQueryException {
        Dataset<Row> df = spark.readStream().format("rate")
                .option("numPartitions", 1)
                .option("rowsPerSecond", 2)
                .load().selectExpr("value as id", "cast(timestamp as string) as name");
        df.createOrReplaceTempView("mem_test_delta_append_only");
        spark.sql("DROP TABLE IF EXISTS test_delta_append_only");
       // spark.sql("CREATE TABLE IF NOT EXISTS test_delta_append_only (id long, name string, DT STRING) USING DELTA PARTITIONED BY (DT)");
        String checkpointDir = getCheckpointDir("test_delta_append_only");
        spark.table("mem_test_delta_append_only").selectExpr("*", "DATE_FORMAT(CURRENT_TIMESTAMP,'yyyyMM') AS DT").createOrReplaceTempView("test_delta_append_only_sql");
        Delta delta1 = new Delta();
        String json = "{ scheduler.pool=pool000001\n" +
                "        checkpointLocation=\"" + checkpointDir + "\"\n" +
                "        isStreaming=true\n" +
                "        table=test_delta_append_only_sql\n" +
                "        partitionBy=DT \n" +
                "        result_table_name=test_delta_append_only\n" +
                "        isAppendOnly=true\n" +
                "        path=/Users/student2020/code/student/SparkDataFlow/spark-warehouse/test_delta_append_only\n" +
                "    }";

        Config config = ConfigFactory.parseString(json).withFallback(configParser.getConfig());
        delta1.setConfig(config);
        delta1.prepare(spark);
        delta1.process(spark);
        delta1.query.awaitTermination(20000);
        //spark.sql("SELECT * FROM test_delta_append_only").show(100, false);
    }
}
