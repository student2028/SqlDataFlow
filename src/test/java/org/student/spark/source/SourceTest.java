package org.student.spark.source;

import org.student.spark.ConfigParser;
import org.student.spark.SparkTestBase;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SourceTest extends SparkTestBase {

//    @BeforeClass
//    public static void setUp() throws IOException {
//        SparkTestBase.setUp();
//        makeKafkaServer();
//    }

    @Test
    public void testFile() {
        Config config = ConfigFactory.load().
                withValue("format", ConfigValueFactory.fromAnyRef("csv"))
                .withValue("path", ConfigValueFactory.fromAnyRef("src/test/resources/test-data/source/csv/test.csv"))
                .withValue("options.header", ConfigValueFactory.fromAnyRef("true"))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("test"));

        File file = new File();
        file.setConfig(config);
        file.prepare(spark);
        file.process(spark);

        System.out.println(file.getConfig());
        System.out.println(file.checkConfig());
        System.out.println(file.getPluginName());
        Dataset<Row> df = spark.table("test");
        df.show(false);
        df.printSchema();

        List<List<Object>> rows = Arrays.asList(
                Arrays.asList("1", "student1", "20"),
                Arrays.asList("2", "student2", "30"),
                Arrays.asList("3", "student3", "6"),
                Arrays.asList("1", "student4", "21"),
                Arrays.asList("2", "student5", "35")
        );

        Dataset<Row> expectedDF = createDataFrame(rows, df.schema());
        assertTrue(assertDataFrameEquals(df, expectedDF));
    }

    @Test
    public void testJdbc() {
        System.setProperty("bigdata.environment", "");

        Jdbc jdbc = new Jdbc();
        Config config = new ConfigParser().getConfig().
                withValue("tag", ConfigValueFactory.fromAnyRef("test")).
                withValue("cached", ConfigValueFactory.fromAnyRef("true"))
                .withValue("table", ConfigValueFactory.fromAnyRef("(select * from utol.cloud_load_log limit 10) as a"))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("sourceJdbcTest"))
                .withFallback(configParser.getConfig());
        jdbc.setConfig(config);
        System.out.println(jdbc.getConfig());
        System.out.println(jdbc.checkConfig());
        System.out.println(jdbc.getPluginName());
        jdbc.prepare(spark);
        jdbc.process(spark);
        jdbc.getDataset(spark).show(false);
        assertTrue(spark.catalog().tableExists("sourceJdbcTest"));
    }

    @Test
    public void testJdbc2() {
        Jdbc jdbc = new Jdbc();
        Config config = new ConfigParser().getConfig()
                .withValue("bigdata.environment", ConfigValueFactory.fromAnyRef("\"\""))
                .withValue("tag", ConfigValueFactory.fromAnyRef("test"))
                .withValue("cached", ConfigValueFactory.fromAnyRef("true"))
                .withValue("partitionColumn", ConfigValueFactory.fromAnyRef("COLNO"))
                .withValue("lowerBound", ConfigValueFactory.fromAnyRef(0))
                .withValue("upperBound", ConfigValueFactory.fromAnyRef(24))
                .withValue("numPartitions", ConfigValueFactory.fromAnyRef(2))
                .withValue("table", ConfigValueFactory.fromAnyRef(
                        "(select * from syscat.columns limit 10) as a"))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("sourceJdbcTest2"));
        jdbc.setConfig(config);
        System.out.println(jdbc.getConfig());
        System.out.println(jdbc.checkConfig());
        System.out.println(jdbc.getPluginName());
        jdbc.prepare(spark);
        jdbc.process(spark);
        jdbc.getDataset(spark).show(false);

        assertTrue(spark.catalog().tableExists("sourceJdbcTest2"));
    }

    @Test
    public void dummyTest() {
        Dummy dummy = new Dummy();
        Config config = ConfigFactory.empty();
        config = config.withValue("limit", ConfigValueFactory.fromAnyRef(20))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("sourceDummyTest"))
        ;
        dummy.setConfig(config);
        dummy.prepare(spark);
        dummy.process(spark);

        System.out.println(dummy.getConfig());
        System.out.println(dummy.checkConfig());
        System.out.println(dummy.getPluginName());
        dummy.getDataset(spark).show(false);
        assertTrue(spark.catalog().tableExists("sourceDummyTest"));
    }


    @Test
    public void stringDataTest() {
        StringData strdata = new StringData();
        Config config = ConfigFactory.empty();
        config = config.withValue("data", ConfigValueFactory.fromAnyRef("id|name\n" +
                "1|student1\n" +
                "2|student2\n" +
                "3|student3"))
                .withValue("format", ConfigValueFactory.fromAnyRef("csv"))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("csv_str_data"))
                .withValue("options.delimiter", ConfigValueFactory.fromAnyRef("|"));
        strdata.setConfig(config);
        strdata.prepare(spark);
        strdata.process(spark);

        strdata.getDataset(spark).show(false);
        assertTrue(spark.catalog().tableExists("csv_str_data"));

    }


    @Test
    public void stringJsonDataTest() {
        StringData strdata = new StringData();
        Config config = ConfigFactory.empty();
        config = config.withValue("data", ConfigValueFactory.fromAnyRef("" +
                "[{\"id\":1,\"name\":\"studentyao\"}" +
                ",{\"id\":2,\"name\":\"student2\"}" +
                "]"))
                .withValue("format", ConfigValueFactory.fromAnyRef("json"))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("json_str_data"))
                .withValue("options.delimiter", ConfigValueFactory.fromAnyRef("|"));
        strdata.setConfig(config);
        strdata.prepare(spark);
        strdata.process(spark);
        System.out.println(strdata.getPluginName());
        System.out.println(strdata.getConfig());
        strdata.getDataset(spark).show(false);
        assertTrue(spark.catalog().tableExists("json_str_data"));
    }

    @Test
    public void RateStreamTest() throws TimeoutException, StreamingQueryException {
        RateStream rs = new RateStream();

        Config config = ConfigFactory.empty();
        config = config.withValue("result_table_name", ConfigValueFactory.fromAnyRef("rateTest")
        );
        rs.setConfig(config);
        rs.prepare(spark);
        rs.process(spark);
        System.out.println(rs.getPluginName());
        System.out.println(rs.getConfig());

        StreamingQuery query = rs.getDataset(spark).writeStream().format("console").start();
        query.awaitTermination(5000);
        assertTrue(spark.catalog().tableExists("rateTest"));
    }

    @Test
    public void testKafkaSourcebBatch() {

        Kafka kafka = new Kafka();
        String configStr =
                "   {\n" +
                        "    isStreaming=false\n" +
                        "    kafka.bootstrap.servers=\"localhost:9083\"\n" +
                        "    topics=\"sdf_plain_test\"\n" +
                        "    result_table_name=kafka_batch_read\n" +
                        "    plainText=true\n" +
                        " }";

        Config config = ConfigFactory.parseString(configStr);
        kafka.setConfig(config);
        System.out.println(kafka.getConfig());
        System.out.println(kafka.getPluginName());
        System.out.println(kafka.checkConfig());

        kafka.prepare(spark);
        kafka.process(spark);

        spark.table("kafka_batch_read").show(10, false);
        assertTrue(spark.catalog().tableExists("kafka_batch_read"));
    }

    @Test
    public void testKafkaSourcebBatchWithSchemaRegistry() {

        Kafka kafka = new Kafka();
        String configStr =
                "   {\n" +
                        "    isStreaming=false\n" +
                        "    kafka.bootstrap.servers=\"localhost:9083\"\n" +
                        "    topics=\"sdf_avro_test\"\n" +
                        "    result_table_name=kafka_batch_read_c_avro\n" +
                        "    schemaRegistryServer=\"http://localhost:18081\"\n" +
                        " }";

        Config config = ConfigFactory.parseString(configStr);
        kafka.setConfig(config);
        System.out.println(kafka.getConfig());
        System.out.println(kafka.getPluginName());
        System.out.println(kafka.checkConfig());

        kafka.prepare(spark);
        kafka.process(spark);

        spark.table("kafka_batch_read_c_avro").show(10, false);
        assertTrue(spark.catalog().tableExists("kafka_batch_read_c_avro"));
    }


    @Test
    @Ignore
    public void testABKafkaSourceStreaming() {

        String json = "spark.structured.streaming=true\n" +
                "source {\n" +
                " kafka {\n" +
                "    isStreaming=true\n" +
                "    kafka.bootstrap.servers=\"localhost:9083\"\n" +
                "    topics=\"sdf_plain_test\"\n" +
                "    result_table_name=kafkatest\n" +
                "    consumer.failOnDataLoss=false\n" +
                "    consumer.startingOffsets=earliest\n" +
                "    consumer.kafka.security.protocol=PLAINTEXT\n" +
                "    consumer.kafka.sasl.mechanism=GSSAPI\n" +
                "    consumer.kafka.ssl.protocol=TLS\n" +
                " }\n" +
                "}\n" +
                "sink{\n" +
                "      stdout2 {\n" +
                "             isStreaming=true\n" +
                "             checkpointLocation=\"/tmp/sparkdataflow/kafka/sdf_plain_test/\"\n" +
                "             table=kafkatest\n" +
                "         }\n" +
                "}\n";
        Config config = ConfigFactory.parseString(json);
        ConfigParser configParser = new ConfigParser(config);
        batchProcess(spark, configParser);

        assertTrue(spark.catalog().tableExists("kafkatest"));
    }


    @Test
    public void testSetVarJava() {
        SetVar set = new SetVar();
        String configStr =
                "   {\n" +
                        " type=java\n" +
                        "      code=\"\"\" " +
                        "      public java.util.Map/*<String,Object>*/ getTest(){\n" +
                        "      java.util.Map/*<String,Object>*/ map = new java.util.HashMap/*<>*/();\n" +
                        "      map.put(\"TESTJAVAVAR\",\"test test test\");\n" +
                        "      return map;" +
                        "      }" +
                        "          \"\"\"\n" +
                        "      result_table_name=testsetvarjava" +
                        " }";

        Config config = ConfigFactory.parseString(configStr);
        set.setConfig(config);
        set.prepare(spark);
        set.getConfig();
        set.getPluginName();
        set.process(spark);
        String result = spark.conf().get("TESTJAVAVAR");
        assertEquals("test test test", result);
    }

    @Test
    public void testSetVarSql() {
        SetVar set = new SetVar();
        String configStr =
                "   {\n" +
                        " type=sql\n" +
                        "      sql=\"\"\" " +
                        "      select \"test test test\" as TEST " +
                        "          \"\"\"\n" +
                        "      result_table_name=testsetvarsql" +
                        " }";

        Config config = ConfigFactory.parseString(configStr);
        set.setConfig(config);
        set.prepare(spark);
        set.getConfig();
        set.getPluginName();
        set.process(spark);
        assertEquals("test test test", spark.conf().get("TEST"));
    }

    @Test
    public void testSetVarJdbc() {
        SetVar set = new SetVar();
        String configStr =
                "   {  bigdata.environment=\"\"\n" +
                        " type=jdbc\n" +
                        "      sql=\"\"\" " +
                        "      select 'test test test' as testv1, 'test2' as testv2 " +
                        "          \"\"\"\n" +
                        "      result_table_name=testsetvarjdbc\n" +
                        "      tag=test" +
                        " }";

        Config config = ConfigFactory.parseString(configStr);
        set.setConfig(config);
        set.prepare(spark);
        set.getConfig();
        set.getPluginName();
        set.process(spark);
        assertEquals("test test test", spark.conf().get("testv1".toUpperCase()));
        assertEquals("test2", spark.conf().get("testv2".toUpperCase()));
    }

}
