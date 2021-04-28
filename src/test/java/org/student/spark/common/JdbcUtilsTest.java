package org.student.spark.common;

import org.student.spark.SparkTestBase;
import org.student.spark.source.Dummy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.types.DataTypes;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import scala.collection.Seq;

import java.util.*;

import static org.junit.Assert.*;

public class JdbcUtilsTest extends SparkTestBase {

    static JdbcOptionsInWrite options;
    static JdbcOptionsInWrite options2;

    @BeforeClass
    public static void setup() {

        String tag = "test";

        scala.collection.immutable.Map<String, String> dataMap = new scala.collection.immutable.HashMap<>();
        dataMap = dataMap.$plus(new scala.Tuple2<>("url", configParser.getConfig().getString(Constants.JDBC_PREFIX + tag + ".url")));
        dataMap = dataMap.$plus(new scala.Tuple2<>("driver", "org.postgresql.Driver"));
        dataMap = dataMap.$plus(new scala.Tuple2<>("dbtable", "test3"));
        dataMap = dataMap.$plus(new scala.Tuple2<>("batchSize", String.valueOf(1)));
        dataMap = dataMap.$plus(new scala.Tuple2<>("numPartitions", "1"));
        options = new JdbcOptionsInWrite(dataMap);

        scala.collection.immutable.Map<String, String> dataMap2 = dataMap;
        dataMap2 = dataMap2.$plus(new scala.Tuple2<>("dbtable", "notexiststable"));
        options2 = new JdbcOptionsInWrite(dataMap2);

    }

    @Test
    public void testDeleteTable() {
        String tag = "test";
        List<List<Object>> rows = Collections.singletonList(
                Arrays.asList("1", "1")
        );
        Dataset<Row> df = createDataFrame(rows, createStructType("id name"));
        df = df.withColumn("id", df.col("id").cast(DataTypes.IntegerType));

        Seq<String> priKeys = scala.collection.JavaConverters.asScalaBuffer(Collections.singletonList("id")).seq();
        String beforedelete = JdbcUtils.query(configParser.getConfig(), tag, "select * from test3").toString();
        System.out.println("before delete:" + beforedelete);
        JdbcUtils.deleteTable(configParser.getConfig(), tag, options, df, priKeys);

        String actual = JdbcUtils.query(configParser.getConfig(), tag, "select * from test3").toString();
        System.out.println("after delete:" + actual);
        assertFalse(actual.contains("{ID=1, NAME=1}"));

    }

    @Test
    public void testQueryTableColumns() {

        Map<String, String> map = JdbcUtils.queryTableColumns(configParser.getConfig(), "test",
                "ENTMT.MNGED_ENTMT");

        System.out.println(map);
        assertTrue(map.containsKey("SAP_UOM_CODE"));
        assertTrue(map.containsKey("NET_SBSCRPTN_ID_QTY"));
        assertTrue(map.containsKey("SAP_EXTRCT_DATE"));

    }

    @Test(expected = DataFlowException.class)
    public void testExecuteSql() {

        String tag = "test";
        String sql = "select * from test.cloud_load_log ";
        JdbcUtils.executeSql(configParser.getConfig(), tag, sql);
    }

    @Test
    public void testExecuteSql2() {

        String tag = "test";
        String sql = "SELECT TABNAME,COLNAME,COLNO,TYPENAME,LENGTH,SCALE,NULLS FROM SYSCAT.COLUMNS\n" +
                " WHERE UPPER('test.test') = TRIM(TABSCHEMA) || '.' || TRIM(TABNAME) \n ";
        assertEquals(JdbcUtils.query(configParser.getConfig(), tag, sql).size(),2);

    }

    @Test(expected = DataFlowException.class)
    public void testTruncateTable() {

        String tag = "test";
        JdbcUtils.truncateTable(configParser.getConfig(), tag, options);
        JdbcUtils.truncateTable(configParser.getConfig(), tag, options2);

    }


    @Test
    public void testMergeTable() {
        System.setProperty("bigdata.environment","");
        List<List<Object>> rows = Arrays.asList(
                Arrays.asList("1", "student1xxxxxxxxxx"),
                Arrays.asList("2", "student2xxxxxxxxxx"),
                Arrays.asList("3", "student3xxxxxxxxxx")
        );
        Dataset<Row> df = createDataFrame(rows, createStructType("ID NAME"));
        df = df.withColumn("id", df.col("id").cast(DataTypes.IntegerType));
        df.show(false);
        String tag = "test";
        String driver = configParser.getConfig().getString("app.jdbc." + tag + ".driver");
        scala.collection.immutable.Map<String, String> dataMap = new scala.collection.immutable.HashMap<>();
        dataMap = dataMap.$plus(new scala.Tuple2<>("url", configParser.getConfig().getString(Constants.JDBC_PREFIX + tag + ".url")));
        dataMap = dataMap.$plus(new scala.Tuple2<>("driver", driver));
        dataMap = dataMap.$plus(new scala.Tuple2<>("dbtable", "test.test5"));
        dataMap = dataMap.$plus(new scala.Tuple2<>("batchSize", String.valueOf(1)));
        dataMap = dataMap.$plus(new scala.Tuple2<>("numPartitions", "1"));
        JdbcOptionsInWrite options = new JdbcOptionsInWrite(dataMap);
        Dataset<Row> df2 = df.filter("length(name)<200");
        Seq<String> priKeys = scala.collection.JavaConverters.asScalaBuffer(Collections.singletonList("ID")).seq();
        JdbcUtils.mergeTable(configParser.getConfig(), tag, options, df2, priKeys);
        assertTrue(JdbcUtils.query(configParser.getConfig(),"test","select * from test.test5")
        .toString().contains("student1xxxxxxxxxx"));

    }

    @Test
    public void testMergeJdbcTable() {
        System.setProperty("bigdata.environment","");
        List<List<Object>> rows = Arrays.asList(
                Arrays.asList("11", "student1xxxxxxxxxx"),
                Arrays.asList("12", "student2xxxxxxxxxx"),
                Arrays.asList("777", "777")
        );
        Dataset<Row> df = createDataFrame(rows, createStructType("ID NAME"));
        df = df.withColumn("id", df.col("id").cast(DataTypes.IntegerType));
        df.show(false);
        String tag = "test";
        String driver = configParser.getConfig().getString("app.jdbc." + tag + ".driver");
        scala.collection.immutable.Map<String, String> dataMap = new scala.collection.immutable.HashMap<>();
        dataMap = dataMap.$plus(new scala.Tuple2<>("url", configParser.getConfig().getString(Constants.JDBC_PREFIX + tag + ".url")));
        dataMap = dataMap.$plus(new scala.Tuple2<>("driver", driver));
        dataMap = dataMap.$plus(new scala.Tuple2<>("dbtable", "test.test4"));
        dataMap = dataMap.$plus(new scala.Tuple2<>("batchSize", String.valueOf(1)));
        dataMap = dataMap.$plus(new scala.Tuple2<>("numPartitions", "1"));
        JdbcOptionsInWrite options = new JdbcOptionsInWrite(dataMap);
        Map<String, String> map = new HashMap<>();
        Dataset<Row> df2 = df.filter("length(name)<200");
        Seq<String> priKeys = scala.collection.JavaConverters.asScalaBuffer(Collections.singletonList("ID")).seq();
        JdbcUtils.mergeJdbcTable(configParser.getConfig(), tag, options, df2, priKeys);
        String actual = JdbcUtils.query(configParser.getConfig(), tag, "select * from test.test4").toString();
        System.out.println(actual);
        assertTrue(actual.contains("{ID=777, NAME=777}"));

    }

    @Test
    @Ignore
    public void testMergeTableDB2() {
        System.setProperty("bigdata.environment","");
        List<List<Object>> rows = Arrays.asList(
                Arrays.asList("1", "student1xxxxxxxxxx"),
                Arrays.asList("2", "student2xxxxxxxxxx"),
                Arrays.asList("3", "student3xxxxxxxxxx")
        );
        Dataset<Row> df = createDataFrame(rows, createStructType("ID NAME"));
        df.show(false);
        String tag = "datavt";
        String driver = configParser.getConfig().getString("app.jdbc." + tag + ".driver");
        scala.collection.immutable.Map<String, String> dataMap = new scala.collection.immutable.HashMap<>();
        dataMap = dataMap.$plus(new scala.Tuple2<>("url", configParser.getConfig().getString(Constants.JDBC_PREFIX + tag + ".url")));
        dataMap = dataMap.$plus(new scala.Tuple2<>("driver", driver));
        dataMap = dataMap.$plus(new scala.Tuple2<>("dbtable", "test.test"));
        dataMap = dataMap.$plus(new scala.Tuple2<>("batchSize", String.valueOf(1)));
        dataMap = dataMap.$plus(new scala.Tuple2<>("numPartitions", "1"));
        JdbcOptionsInWrite options = new JdbcOptionsInWrite(dataMap);
        Dataset<Row> df2 = df.filter("length(name)<200");
        Seq<String> priKeys = scala.collection.JavaConverters.asScalaBuffer(Collections.singletonList("ID")).seq();
        JdbcUtils.mergeTable(configParser.getConfig(), tag, options, df2, priKeys);
        System.out.println(JdbcUtils.query(configParser.getConfig(),"datavt","select * from test.test"));
    }

    @Test
    public void testInsertTable() {
        List<List<Object>> rows = Arrays.asList(
                Arrays.asList("1", "1"),
                Arrays.asList("2", "2"),
                Arrays.asList("888", "6")
        );
        Dataset<Row> df = createDataFrame(rows, createStructType("id name"));
        df = df.withColumn("id", df.col("id").cast(DataTypes.IntegerType));

        df.show(false);
        String tag = "test";
        JdbcUtils.insertTable(configParser.getConfig(), tag, options, df);
        String actual = JdbcUtils.query(configParser.getConfig(), tag, "select * from test3").toString();
        System.out.println(actual);
        assertTrue(actual.contains("{ID=888, NAME=6}"));
    }

    @Test
    public void testUpsertTable() {

        List<List<Object>> rows = Arrays.asList(
                Arrays.asList("1", "1"),
                Arrays.asList("8", "8"),
                Arrays.asList("7", "7")
        );
        Dataset<Row> df = createDataFrame(rows, createStructType("id name"));
        df = df.withColumn("id", df.col("id").cast(DataTypes.IntegerType));

        df.show(false);
        String tag = "test";
        String sql = "insert into test3(id,name) values(?,?)";

        System.out.println(JdbcUtils.query(configParser.getConfig(), tag, "select * from test3").toString());

        JdbcUtils.upsertTable(configParser.getConfig(), tag, options, df, sql);

        assertTrue(JdbcUtils.query(configParser.getConfig(), tag, "select * from test3").toString()
        .contains("{ID=8, NAME=8}"));

    }


    private void makeSource() {
        Dummy dummy = new Dummy();
        Config config = ConfigFactory.empty().
                withValue("limit", ConfigValueFactory.fromAnyRef(20)).withValue(
                "result_table_name", ConfigValueFactory.fromAnyRef("test")
        );
        dummy.setConfig(config);
        dummy.prepare(spark);
        dummy.process(spark);
    }

    @Test
    public void testMetaUtils() {

        makeSource();
        JdbcUtils.getSetters(spark.table("test").schema(), null);

    }


}