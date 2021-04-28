package org.student.spark.sink;

import org.student.spark.ConfigParser;
import org.student.spark.SparkTestBase;
import org.student.spark.api.Plugin;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.JdbcUtils;
import org.student.spark.source.Dummy;
import org.student.spark.source.StringData;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;
import za.co.absa.abris.config.AbrisConfig;
import za.co.absa.abris.config.FromAvroConfig;

import java.util.Collections;
import java.util.HashMap;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static za.co.absa.abris.avro.functions.from_avro;

public class SinkTest extends SparkTestBase {

//    @BeforeClass
//    public static void setUp() throws IOException {
//        SparkTestBase.setUp();
//        makeKafkaServer();
//    }

    @Test
    public void testFile() {
        makeSource();
        File file = new File();
        Config config = ConfigFactory.empty().
                withValue("format", ConfigValueFactory.fromAnyRef("csv"))
                .withValue("path", ConfigValueFactory.fromAnyRef("/tmp/sdf/sink/csv/"))
                .withValue("table", ConfigValueFactory.fromAnyRef("test"))
                .withValue("options.delimiter", ConfigValueFactory.fromAnyRef("|"))
                .withValue("options.header", ConfigValueFactory.fromAnyRef("true"))
                .withValue("options.compression", ConfigValueFactory.fromAnyRef("gzip"))
                .withValue("savemode", ConfigValueFactory.fromAnyRef("overwrite"));

        file.setConfig(config);
        System.out.println(file.getConfig());
        System.out.println(file.getPluginName());
        System.out.println(file.checkConfig());
        file.prepare(spark);
        file.process(spark);

        assertEquals(spark.read().format("csv").option("header", "true").load("/tmp/sdf/sink/csv/")
                .count(), 200);
    }


    @Test
    public void testConsole() {
        makeSource();
        Console console = new Console();
        Config config2 = ConfigFactory.empty();
        config2 = config2.withValue("limit", ConfigValueFactory.fromAnyRef(200))
                .withValue("table", ConfigValueFactory.fromAnyRef("test"));
        console.setConfig(config2);
        System.out.println(config2.root().render());

        System.out.println(console.checkConfig());
        System.out.println(console.getConfig());
        System.out.println(console.getPluginName());

        console.prepare(spark);
        console.process(spark);
    }

    @Test
    public void testConsole2() throws StreamingQueryException {

        makeStreamSource("11111", "11111", "testconsolestream");
        String json = "{\n" +
                "            trigger_type=ProcessingTime\n" +
                "            interval=\"1 seconds\"\n" +
                "            isStreaming=true\n" +
                "            limit = 20\n" +
                "            table=testconsolestream\n" +
                "        }\n";
        Config config = ConfigFactory.parseString(json);
        Console console = new Console();
        console.setConfig(config);
        console.prepare(spark);
        console.process(spark);
        console.query.awaitTermination(5000);
    }

    private void makeSource() {
        Dummy dummy = new Dummy();
        Config config = ConfigFactory.empty().
                withValue("limit", ConfigValueFactory.fromAnyRef(200)).withValue(
                "result_table_name", ConfigValueFactory.fromAnyRef("test")
        );
        dummy.setConfig(config);
        dummy.prepare(spark);
        dummy.process(spark);
    }

    private void makeSource2() {
        StringData strdata = new StringData();
        Config config = ConfigFactory.empty();
        config = config.withValue("data", ConfigValueFactory.fromAnyRef("" +
                "[{\"id\":1,\"name\":\"studentyao\"}" +
                ",{\"id\":2,\"name\":\"student2\"}" +
                "]"))
                .withValue("format", ConfigValueFactory.fromAnyRef("json"))
                .withValue("result_table_name", ConfigValueFactory.fromAnyRef("test2"))
                .withValue("options.delimiter", ConfigValueFactory.fromAnyRef("|"));
        strdata.setConfig(config);
        strdata.prepare(spark);
        strdata.process(spark);
    }

    @Test
    public void testEmail() {

        Dataset<Row> df = spark.createDataset(Collections.singletonList("hello world,hello spark,hello sparkdataflow"), Encoders.STRING()).toDF("msg");
        df.createOrReplaceTempView("test");
        df.show(10, false);
        Email email = new Email();
        Config config = ConfigFactory.empty().
                withValue("table", ConfigValueFactory.fromAnyRef("test")
                );
        email.setConfig(config);
        System.out.println(email.getConfig());
        System.out.println(email.checkConfig());
        System.out.println(email.getPluginName());

        email.prepare(spark);
        email.process(spark);
    }

    @Test
    public void testJdbc1() {
        makeSource2();
        Jdbc jdbc1 = new Jdbc();
        String json = "{     bigdata.environment=\"\"\n" +
                "            tag=test\n" +
                "            table=test2\n" +
                "            result_table_name=\"test.test6\"\n" +
                "            savemode=overwrite\n" +
                "     }";

        Config config = ConfigFactory.parseString(json).withFallback(configParser.getConfig());

        System.out.println(config);
        jdbc1.setConfig(config);
        System.out.println(jdbc1.getPluginName());
        System.out.println(jdbc1.getConfig());
        System.out.println(jdbc1.checkConfig());

        jdbc1.prepare(spark);
        jdbc1.process(spark);

        assertTrue(JdbcUtils.query(config, "test", "select * from test.test6")
                .toString().contains("ID=1, NAME=studentyao")
        );

    }

    @Test
    public void testPostgresql1() {
        makeSource2();
        Postgresql pg1 = new Postgresql();
        String json = " { \n" +
                "        bigdata.environment=\"\"\n" +
                "        tag=test\n" +
                "        table=test2\n" +
                "        result_table_name= \"test.test\"\n" +
                "        sql=\"\"\"\n" +
                "        insert into test.test(id,name)\n" +
                "        values(?,?)\n" +
                "        ON CONFLICT (id) DO UPDATE\n" +
                "        set name=excluded.name \"\"\" \n" +
                "}";

        Config config = ConfigFactory.parseString(json).withFallback(configParser.getConfig());

        System.out.println(config);
        pg1.setConfig(config);
        System.out.println(pg1.getPluginName());
        System.out.println(pg1.getConfig());
        System.out.println(pg1.checkConfig());

        pg1.prepare(spark);
        pg1.process(spark);

        assertTrue(
                JdbcUtils.query(config, "test", "select * from test.test")
                        .toString().contains("ID=1, NAME=studentyao")
        );

    }

    @Test
    public void testPostgresqlStreaming() throws StreamingQueryException {
        makeStreamSource("3333333", "3333333", "test3333333");
        Postgresql pg1 = new Postgresql();
        String json = " { scheduler.pool=pool1\n" +
                "         bigdata.environment=\"\"\n" +
                "         tag=test\n" +
                "         isStreaming=true\n" +
                "         checkpointLocation=\"/tmp/sdf/testpg\"\n" +
                "         table=test3333333\n" +
                "         result_table_name= \"test.test3\"\n" +
                "         sql=\"\"\"\n" +
                "         insert into test.test3(id,name)\n" +
                "         values(?,?)\n" +
                "         ON CONFLICT (id) DO UPDATE\n" +
                "         set name=excluded.name \"\"\" \n" +
                "}";

        Config config = ConfigFactory.parseString(json).withFallback(configParser.getConfig());
        pg1.setConfig(config);
        pg1.prepare(spark);
        pg1.process(spark);
        pg1.query.awaitTermination(10000);
        int size = JdbcUtils.query(config, "test", "select * from test.test3 where id=3333333").size();
        assertEquals(size, 1);
    }

    @Test
    public void testPostgresql3() {
        makeSource2();
        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql:postgres")
                .option("dbtable", "(select 'test.test' as topic," +
                        " 'select id,name from csv.`src/test/resources/test-data/source/csv/test.csv`'" +
                        " as sql,'21' as version) as t")
                .option("user", "postgres")
                .option("password", "postgres")
                .load();

        df.createOrReplaceTempView("clltest");

        Postgresql pg1 = new Postgresql();
        String json = " {\n" +
                "           bigdata.environment=\"\"\n" +
                "           metaTag=test\n" +
                "           tag=test\n" +
                "           table=test2\n" +
                "           numPartitions=1\n" +
                "           result_table_name= \"test.test\"\n" +
                "           type=default\n" +
                "           metaSource=clltest\n" +
                "           topic=\"test.test\"\n" +
                "         sql=\"\"\"\n" +
                "         insert into test.test(id,name)\n" +
                "         values(?,?)\n" +
                "         ON CONFLICT (id) DO UPDATE\n" +
                "         set name=excluded.name \"\"\" \n" +
                "   }";

        Config config = ConfigFactory.parseString(json).withFallback(configParser.getConfig());

        System.out.println(config);
        pg1.setConfig(config);
        System.out.println(pg1.getPluginName());
        System.out.println(pg1.getConfig());
        System.out.println(pg1.checkConfig());
        pg1.prepare(spark);
        pg1.process(spark);

    }


    @Test
    public void testAAKafkaSinkBatch() {
        Dataset<Row> df = spark.range(1, 100).map((MapFunction<Long, String>) i
                ->
                String.valueOf(i), Encoders.STRING()).toDF("value");
        df.createOrReplaceTempView("sdf_batch_test");

        String json = "{ " +
                "    plainText=true\n" +
                "    isStreaming=false\n" +
                "    kafka.bootstrap.servers=\"localhost:9083\"\n" +
                "    topics=\"sdf_kafka_sink_test\"\n" +
                "    table=sdf_batch_test\n" +
                "    producer.kafka.security.protocol=PLAINTEXT\n" +
                "    producer.kafka.sasl.mechanism=GSSAPI\n" +
                "    producer.kafka.ssl.protocol=TLS\n" +
                " }";
        Config config = ConfigFactory.parseString(json);
        Kafka kafka = new Kafka();
        kafka.setConfig(config);
        System.out.println(kafka.getConfig());
        System.out.println(kafka.getPluginName());
        System.out.println(kafka.checkConfig());

        kafka.prepare(spark);
        kafka.process(spark);

        Dataset<Row> df2 = spark.read().format("kafka").option("kafka.bootstrap.servers", "localhost:9083")
                .option("subscribe", "sdf_kafka_sink_test")
                .option("consumer.startingOffsets", "earliest")
                .load().select(col("offset").as("OFFSET"),
                        (col("value")));
        df2.show(10, false);

    }

    @Test
    public void testAAKafkaSinkBatchWithSchenaRegistry() {
        Dataset<Row> df2 = spark.range(200, 210).map(
                (MapFunction<Long, Tuple2<Long, String>>) i ->
                        new Tuple2<>(i, String.valueOf(i)),
                Encoders.tuple(Encoders.LONG(), Encoders.STRING())
        ).toDF("id", "name");
        df2.createOrReplaceTempView("sdf_batch_test2");

        String json = "{ " +
                "    plainText=false\n" +
                "    isStreaming=false\n" +
                "    kafka.bootstrap.servers=\"localhost:9083\"\n" +
                "    topics=\"sdf_avro_test\"\n" +
                "    table=sdf_batch_test2\n" +
                "    producer.kafka.security.protocol=PLAINTEXT\n" +
                "    producer.kafka.sasl.mechanism=GSSAPI\n" +
                "    producer.kafka.ssl.protocol=TLS\n" +
                "    schemaRegistryServer=\"http://localhost:18081/\"" +
                " }";
        Config config = ConfigFactory.parseString(json);
        Kafka kafka = new Kafka();
        kafka.setConfig(config);
        System.out.println(kafka.getConfig());
        System.out.println(kafka.getPluginName());
        System.out.println(kafka.checkConfig());

        kafka.prepare(spark);
        kafka.process(spark);

        FromAvroConfig fromAvroConfig = AbrisConfig.fromConfluentAvro()
                .downloadReaderSchemaByLatestVersion()
                .andTopicNameStrategy("sdf_avro_test", false)
                .usingSchemaRegistry("http://localhost:18081/");


        Dataset<Row> df = spark.read().format("kafka").option("kafka.bootstrap.servers", "localhost:9083")
                .option("subscribe", "sdf_avro_test")
                .option("consumer.startingOffsets", "earliest")
                .load().select(col("offset").as("OFFSET"),
                        from_avro(col("value"), fromAvroConfig).alias("__temp__"))
                .select("OFFSET", "__temp__.*").filter("id>200 and id<206");
        df.show(10, false);
        assertEquals(df.count(), 5);

    }


    @Test
    public void testACKafkaSinkStreaming() throws StreamingQueryException {
        makeStreamSource("7777777", "7777777", "dummytest2");
        String json = "{ plainText=false\n" +
                "    checkpointLocation=/tmp/sdf/kfksink\n" +
                "    isStreaming=true\n" +
                "    kafka.bootstrap.servers=\"localhost:9083\"\n" +
                "    topics=\"sdf_topic2\"\n" +
                "    table=dummytest2\n" +
                "    producer.kafka.security.protocol=PLAINTEXT\n" +
                "    producer.kafka.sasl.mechanism=GSSAPI\n" +
                "    producer.kafka.ssl.protocol=TLS\n" +
                " }\n";
        Config config = ConfigFactory.parseString(json);
        Kafka kafka = new Kafka();
        kafka.setConfig(config);
        kafka.prepare(spark);
        kafka.process(spark);
        kafka.query.awaitTermination(5000);
        Dataset<Row> df = spark.read().format("kafka").option("kafka.bootstrap.servers", "localhost:9083")
                .option("subscribe", "sdf_topic2")
                .option("consumer.startingOffsets", "earliest")
                .load().select(col("offset").as("OFFSET"),
                        (col("value")));
        df.show(10, false);
    }

    @Test
    public void testDBSinkStreaming() throws StreamingQueryException {
        makeStreamSource("9999", "9999", "test9999");
        DB db1 = new DB();
        String json = " { scheduler.pool=pool1\n" +
                "         bigdata.environment=\"\"\n" +
                "         tag=test\n" +
                "         isStreaming=true\n" +
                "         checkpointLocation=\"/tmp/sdf/testdb\"\n" +
                "         table=test9999\n" +
                "         result_table_name= \"test.test3\"\n" +
                "         sql=\"\"\"\n" +
                "         insert into test.test3(id,name)\n" +
                "         values(?,?)\n" +
                "         ON CONFLICT (id) DO UPDATE\n" +
                "         set name=excluded.name \"\"\" \n" +
                "}";

        Config config = ConfigFactory.parseString(json).withFallback(configParser.getConfig());

        db1.setConfig(config);
        db1.prepare(spark);
        db1.process(spark);
        db1.query.awaitTermination(10000);
        int size = JdbcUtils.query(config, "test", "select * from test.test3 where name='9999'").size();
        System.out.println(JdbcUtils.query(config, "test", "select * from test.test3 where name='9999'"));
        assertEquals(1, size);
    }


    @Test
    @Ignore
    public void testDeltaSinkStreaming2() throws StreamingQueryException {

        spark.sql("DROP TABLE IF EXISTS test_delta8888 ");
        spark.sql("CREATE TABLE  test_delta8888 (id long, name string) USING DELTA");
        spark.sql("DROP TABLE IF EXISTS test_delta7777 ");
        spark.sql("CREATE TABLE  test_delta7777 (id long, name string) USING DELTA");
        spark.sql("DROP TABLE IF EXISTS test_delta6666 ");
        spark.sql("CREATE TABLE  test_delta6666 (id long, name string) USING DELTA");
        spark.sql("DROP TABLE IF EXISTS test_delta5555 ");
        spark.sql("CREATE TABLE  test_delta5555 (id long, name string) USING DELTA");

        makeStreamSource("8888", "8888", "test8888");
        makeStreamSource("7777", "7777", "test7777");
        makeStreamSource("6666", "6666", "test6666");
        makeStreamSource("5555", "5555", "test5555");

        Delta delta8888 = new Delta();
        String json8888 = "{scheduler.pool=pool8888\n" +
                "        checkpointLocation=\"/tmp/sdf/testdelta8/\"\n" +
                "        isStreaming=true\n" +
                "        table=test8888\n" +
                "        result_table_name=newData\n" +
                "        sql=\"\"\"\n" +
                "            MERGE INTO test_delta8888 as t USING ( select id , name  from (select id,name, " +
                "            row_number() over(partition by id order by id)  as rn from newData ) as a where a.rn=1)as s " +
                "            ON t.id=s.id\n" +
                "            WHEN MATCHED THEN\n" +
                "            UPDATE SET *\n" +
                "            WHEN NOT MATCHED THEN INSERT *\n" +
                "       \"\"\"\n" +
                "    }";

        Config config = ConfigFactory.parseString(json8888).withFallback(configParser.getConfig());
        delta8888.setConfig(config);
        Delta delta7777 = new Delta();
        String json7777 = "{scheduler.pool=pool7777\n" +
                "        checkpointLocation=\"/tmp/sdf/testdelta7/\"\n" +
                "        isStreaming=true\n" +
                "        table=test7777\n" +
                "        result_table_name=newData\n" +
                "        sql=\"\"\"\n" +
                "            MERGE INTO test_delta7777 as t USING ( select id , name  from (select id,name, " +
                "            row_number() over(partition by id order by id)  as rn from newData ) as a where a.rn=1)as s " +
                "            ON t.id=s.id\n" +
                "            WHEN MATCHED THEN\n" +
                "            UPDATE SET *\n" +
                "            WHEN NOT MATCHED THEN INSERT *\n" +
                "       \"\"\"\n" +
                "    }";

        Config config7777 = ConfigFactory.parseString(json7777).withFallback(configParser.getConfig());
        delta7777.setConfig(config7777);
        Delta delta6666 = new Delta();
        String json6666 = "{scheduler.pool=pool6666\n" +
                "        checkpointLocation=\"/tmp/sdf/testdelta6/\"\n" +
                "        isStreaming=true\n" +
                "        table=test6666\n" +
                "        result_table_name=newData\n" +
                "        sql=\"\"\"\n" +
                "            MERGE INTO test_delta6666 as t USING ( select id , name  from (select id,name, " +
                "            row_number() over(partition by id order by id)  as rn from newData ) as a where a.rn=1)as s " +
                "            ON t.id=s.id\n" +
                "            WHEN MATCHED THEN\n" +
                "            UPDATE SET *\n" +
                "            WHEN NOT MATCHED THEN INSERT *\n" +
                "       \"\"\"\n" +
                "    }";

        Config config6666 = ConfigFactory.parseString(json6666).withFallback(configParser.getConfig());
        delta6666.setConfig(config6666);
        Delta delta5555 = new Delta();
        String json5555 = "{scheduler.pool=pool5555\n" +
                "        checkpointLocation=\"/tmp/sdf/testdelta5/\"\n" +
                "        isStreaming=true\n" +
                "        table=test5555\n" +
                "        result_table_name=newData\n" +
                "        sql=\"\"\"\n" +
                "            MERGE INTO test_delta5555 as t USING ( select id , name  from (select id,name, " +
                "            row_number() over(partition by id order by id)  as rn from newData ) as a where a.rn=1)as s " +
                "            ON t.id=s.id\n" +
                "            WHEN MATCHED THEN\n" +
                "            UPDATE SET *\n" +
                "            WHEN NOT MATCHED THEN INSERT *\n" +
                "       \"\"\"\n" +
                "    }";

        Config config5555 = ConfigFactory.parseString(json5555).withFallback(configParser.getConfig());
        delta5555.setConfig(config5555);

        delta8888.prepare(spark);
        delta7777.prepare(spark);
        delta6666.prepare(spark);
        delta5555.prepare(spark);

        delta8888.process(spark);
        delta7777.process(spark);
        delta6666.process(spark);
        delta5555.process(spark);

        spark.streams().awaitAnyTermination(100000000);

    }

}
