package org.student.spark.processor;

import org.student.spark.SparkTestBase;
import org.student.spark.source.Dummy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.junit.Assert.*;

public class ProcessorTest extends SparkTestBase {

    @Test
    public void testDrop() {
        Config config = ConfigFactory.empty().
                withValue("table", ConfigValueFactory.fromAnyRef("test"))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("drop"))
                .withValue("source_field", ConfigValueFactory.fromAnyRef(Arrays.asList("id,name".split(","))));

        Drop drop = new Drop();
        drop.setConfig(config);
        drop.prepare(spark);
        drop.process(spark);

        System.out.println(drop.getPluginName());
        System.out.println(drop.getConfig());
        System.out.println(drop.checkConfig());
        spark.table("drop").show(false);

        boolean hasIDOrName = scala.collection.JavaConverters.seqAsJavaListConverter(spark.table("drop").schema().toList()).asJava()
                .stream().anyMatch(f -> f.name().equals("id") || f.name().equals("name"));
        assertFalse(hasIDOrName);

    }

    @Test
    public void testRepartition() {
        Config config = ConfigFactory.empty().
                withValue("table", ConfigValueFactory.fromAnyRef("test"))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("repartition"))
                .withValue("num_partitions", ConfigValueFactory.fromAnyRef(10));

        Repartition repartition = new Repartition();
        repartition.setConfig(config);
        repartition.prepare(spark);
        repartition.process(spark);

        System.out.println(repartition.getPluginName());
        System.out.println(repartition.getConfig());
        System.out.println(repartition.checkConfig());

        assertEquals(spark.table("repartition").rdd().getNumPartitions(), 10);
    }

    @Test
    public void testRename() {
        final String JSON = "{\n" +
                "    \"table\":\"test\",\n" +
                "    \"result_table_name\":\"rename\",\n" +
                "    \"fields\":[\n" +
                "        {\"field\":\"id\", \"new_field\":\"ID_NEW\"},\n" +
                "        {\"field\":\"name\", \"new_field\":\"NAME_NEW\"}\n" +
                "    ]\n" +
                "}";
        System.out.println(JSON);
        Config config = ConfigFactory.parseString(JSON);

        Rename rename = new Rename();
        rename.setConfig(config);
        rename.prepare(spark);
        rename.process(spark);

        System.out.println(rename.getPluginName());
        System.out.println(rename.getConfig());
        System.out.println(rename.checkConfig());

        spark.table("rename").show(false);

        List<StructField> fieldList = scala.collection.JavaConverters.seqAsJavaListConverter(
                spark.table("rename").schema().toList()).asJava();
        boolean hasNewIDAndName = fieldList.stream().anyMatch(f -> f.name().equals("ID_NEW"))
                && fieldList.stream().anyMatch(f -> f.name().equals("NAME_NEW"));
        assertTrue(hasNewIDAndName);

    }

    @Test
    public void testReplace() {
        final String JSON = "{\n" +
                "    \"table\":\"test\",\n" +
                "    \"result_table_name\":\"replace\",\n" +
                "    \"fields\":[\n" +
                "        {\"source_field\":\"name\", \"target_field\":\"NAME_NEW\",\"pattern\":\"Name\",\"replacement\":\"NAME\"},\n" +
                "        {\"source_field\":\"COL_CHAR\", \"target_field\":\"COL_CHAR\",\"pattern\":\"CHAR\",\"replacement\":\"char\"}\n" +
                "     ]\n" +
                "}";
        System.out.println(JSON);
        Config config = ConfigFactory.parseString(JSON);

        Replace replace = new Replace();
        replace.setConfig(config);
        replace.prepare(spark);
        replace.process(spark);

        System.out.println(replace.getPluginName());
        System.out.println(replace.getConfig());
        System.out.println(replace.checkConfig());

        spark.table("replace").show(false);
        assertTrue(spark.catalog().tableExists("replace"));
    }

    @BeforeClass
    public static void makeSource() {
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
    public void testTrim() {
        List<StructField> fields = new ArrayList<>();
        StructField field = DataTypes.createStructField("id", DataTypes.StringType, false);
        fields.add(field);
        StructType schema = DataTypes.createStructType(fields);

        List<List<Object>> rows = Arrays.asList(
                Collections.singletonList("testrow1\r\n\t "),
                Collections.singletonList("testrow2\t\t ")
        );

        Dataset<Row> df = createDataFrame(rows, schema);
        df.createOrReplaceTempView("testrow");

        Trim trim = new Trim();
        Config config = ConfigFactory.empty().
                withValue("table", ConfigValueFactory.fromAnyRef("testrow")).withValue(
                "result_table_name", ConfigValueFactory.fromAnyRef("trim")
        );
        trim.setConfig(config);
        trim.prepare(spark);
        trim.process(spark);

        System.out.println(trim.getPluginName());
        System.out.println(trim.getConfig());
        System.out.println(trim.checkConfig());
        //prepare dataframe after trim
        rows = Arrays.asList(
                Collections.singletonList("testrow1"),
                Collections.singletonList("testrow2")
        );
        Dataset<Row> expectedDF = createDataFrame(rows, schema).withColumn("len", lit(8));

        Dataset<Row> actualDF = spark.table("trim").withColumn("len", length(col("id")));
        assertTrue(assertDataFrameEquals(actualDF, expectedDF));

    }

    @Test
    public void testSql() {
        System.setProperty("bigdata.environment", "");
        Sql sql = new Sql();
        Config appConfig = ConfigFactory.load();
        Config config = ConfigFactory.empty()
                .withValue("sql", ConfigValueFactory.fromAnyRef("select count(*) from test.test1"))
                .withValue("inferSource", ConfigValueFactory.fromAnyRef("true"))
                .withValue("sourceTag", ConfigValueFactory.fromAnyRef("test"))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("sql")
                ).withFallback(appConfig)
                .withValue("bigdata.environment", ConfigValueFactory.fromAnyRef(""));
        sql.setConfig(config);
        sql.prepare(spark);
        sql.process(spark);

        System.out.println(sql.getPluginName());
        System.out.println(sql.getConfig());
        System.out.println(sql.checkConfig());

        assertTrue(spark.catalog().tableExists("sql"));

    }

    @Test
    public void testSql2() {
        System.setProperty("bigdata.environment", "");

        List<List<Object>> rows = Collections.singletonList(
                Arrays.asList("test.test1", "select current_timestamp")
        );
        Dataset<Row> df = createDataFrame(rows, createStructType("topic sql"));
        df.createOrReplaceTempView("cll");

        Sql sql = new Sql();
        Config appConfig = ConfigFactory.load();
        Config config = ConfigFactory.empty().
                withValue("sql", ConfigValueFactory.fromAnyRef("select * from "))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("sql2"))
                .withValue("topic", ConfigValueFactory.fromAnyRef("test.test1"))
                .withValue("table", ConfigValueFactory.fromAnyRef("cll"))
                .withValue("fromTable", ConfigValueFactory.fromAnyRef("true")
                ).withFallback(appConfig);
        sql.setConfig(config);
        sql.prepare(spark);
        sql.process(spark);

        System.out.println(sql.getPluginName());
        System.out.println(sql.getConfig());
        System.out.println(sql.checkConfig());

        assertTrue(spark.catalog().tableExists("sql2"));
    }


    @Test
    public void testSql3() {
        System.setProperty("bigdata.environment", "");

        Sql sql = new Sql();
        String json = "{ app.sql.dir=\"\"\n" +
                "        inferSource=false\n" +
                "        sqlfile= \"src/test/resources/sql/test.sql\"\n" +
                "        result_table_name= \"sqltest3\"\n" +
                "        variables :[ {\"key\": \"$limit\", \"value\": 10} ]" +
                "    }";
        Config config = ConfigFactory.parseString(json).withFallback(configParser.getConfig());
        sql.setConfig(config);
        sql.prepare(spark);
        sql.process(spark);

        System.out.println(sql.getPluginName());
        System.out.println(sql.getConfig());
        System.out.println(sql.checkConfig());

        spark.table("sqltest3").show(false);
        assertTrue(spark.catalog().tableExists("sqltest3"));

    }


}
