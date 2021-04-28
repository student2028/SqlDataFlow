package org.student.spark.common;

import org.student.spark.ConfigParser;
import org.student.spark.SparkTestBase;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.functions.lit;
import static org.junit.Assert.*;

public class CommonUtilsTest extends SparkTestBase {

    Config config = ConfigFactory.parseString("{test=\"test\"\n" +
            " testint=1\n" +
            " testbool=true\n" +
            " app.k8s.secrets.dir=/tmp\n" +
            " app.k8s.secrets.file=test.prop\n" +
            "}\n");

//    @BeforeClass
//    public static void setUp() throws IOException {
//        SparkTestBase.setUp();
//        makeKafkaServer();
//    }

    @Test
    public void getStringSafe() {
        assertEquals(CommonUtils.getStringSafe(config, "test"), "test");
    }

    @Test
    public void testMergeK8sSecrets(){

        Config config = ConfigFactory.empty().withValue(Constants.K8S_SECRETS_ENABLED,ConfigValueFactory.fromAnyRef(true))
                .withValue(Constants.K8S_SECRETS_DIR,ConfigValueFactory.fromAnyRef("src/test/resources/"))
                .withValue(Constants.K8S_SECRETS_FILE,ConfigValueFactory.fromAnyRef("application.conf"));

        config =  CommonUtils.mergeK8sSecrets(config);
        System.out.println(config);
    }

    @Test
    public void getStringWithDefault() {

        assertEquals("test", CommonUtils.getStringWithDefault(config, "test", "test"));
        assertEquals("test", CommonUtils.getStringWithDefault(config, "testnokey", "test"));

    }

    @Test
    public void getStringWithPrefix() {

        assertEquals("/test", CommonUtils.getStringWithPrefix(config, "test", "/"));
        assertEquals("", CommonUtils.getStringWithPrefix(config, "testnokey", "/"));
    }

    @Test
    public void getIntWithDefault() {

        assertEquals(1, CommonUtils.getIntWithDefault(config, "testint", 2));
        assertEquals(2, CommonUtils.getIntWithDefault(config, "testintnokey", 2));

    }

    @Test
    public void getLongWithDefault() {

        assertEquals(1, CommonUtils.getLongWithDefault(config, "testint", 2));
        assertEquals(2, CommonUtils.getLongWithDefault(config, "testintnokey", 2));

    }

    @Test
    public void getBooleanWithDefault() {
        assertTrue(CommonUtils.getBooleanWithDefault(config, "testbool", false));
        assertFalse(CommonUtils.getBooleanWithDefault(config, "testboolnokey", false));

    }

    @Test
    public void normalizeName() {
        String tableName = "test.test";
        assertEquals("test_test", CommonUtils.normalizeName(tableName));
        assertNull(CommonUtils.normalizeName(null));
    }

    @Test
    public void printSQLException() {
        SQLException ex = new SQLException("external exception");
        SQLException iex = new SQLException("internal exception");
        ex.setNextException(iex);
        CommonUtils.printSQLException(ex);
    }


    @Test
    public void regexpExtractAll() {
        String path = "s3a://test_bucket.service/sql/recon/sql1.sql";
        String regex = "(?<=://)(.*?)(?=\\.service/)";
        String regex2 = "(?<=test_bucket.service)(.*)";
        assertEquals("[/sql/recon/sql1.sql]", CommonUtils.regexpExtractAll(path, regex2).toString());
    }

    @Test
    public void extractNumber() {

        assertEquals("1234", CommonUtils.extractNumber("test1234").toString());
        assertEquals("123", CommonUtils.extractNumber("test123").toString());
        assertEquals("12", CommonUtils.extractNumber("test12").toString());
        assertEquals("1", CommonUtils.extractNumber("test1").toString());

    }

    @Test
    public void getStringOctetsLen() {
        String teststr = "12!@";
        int actuallen = CommonUtils.getStringOctetsLen(teststr);
        assertEquals(4, actuallen);
    }

    @Test
    public void castDataset() {
        Dataset<Row> ds = createKVDataSet(
                Arrays.asList(tuple2(1, 100), tuple2(3, 300)), "id", "name");

        Dataset<Row> ds2 = ds.withColumn("dt", lit("2020-10-10 10:10:10.123"));

        Map<String, String> schema = new HashMap<String, String>() {{
            put("id", "integer");
            put("name", "varchar(2)");
            put("dt", "timestamp");
        }};

        Dataset<Row> dsnew = CommonUtils.castDataset(ds2, schema);
        String expected = "StructType(StructField(id,IntegerType,true), StructField(name,StringType,true), StructField(dt,TimestampType,true))";
        assertEquals(expected, dsnew.schema().toString());
    }

    @Test
    public void loadFile() {
        //test from local
        String filePath = "src/test/resources/sql/fixed.sql";
        String content = CommonUtils.loadFile(filePath);
        String expected = "select * from test";
        assertEquals(expected, content);

    }

    @Test
    public void nullSafeJoin() {

        Dataset<Row> df1 = spark.range(10).toDF("col1");
        Dataset<Row> df2 = spark.range(10).toDF("col1");

        Column col = new Column("(d2.col1 <=> old.col1)");
        assertEquals(col.toString(), CommonUtils.nullSafeJoin(df1, df2).toString());

    }

    @Test
    public void extractTables() {
        String sql = "select \"dwdm1.actv_incent_vad\" AS srctopic, A_CCID from dwdm1.actv_incent_vad";
        Set<String> tables = CommonUtils.extractTables(sql, spark);
        String re1 = "";
        String expected = "select \"dwdm1.actv_incent_vad\" AS srctopic, A_CCID from dwdm1_actv_incent_vad";
        for (String table : tables) {
            re1 = sql.replaceAll("(?<!['\"])" + table + "(?!['\"])", CommonUtils.normalizeName(table));
        }
        assertEquals(expected, re1);

        String sql2 = "select ' dwdm1.actv_incent_vad ' AS srctopic, A_CCID from dwdm1.actv_incent_vad";
        String re2 = "";
        String expected2 = "select ' dwdm1_actv_incent_vad ' AS srctopic, A_CCID from dwdm1_actv_incent_vad";
        tables = CommonUtils.extractTables(sql2, spark);
        for (String table : tables) {
            re2 = sql2.replaceAll("(?<!['\"])" + table + "(?!['\"])", CommonUtils.normalizeName(table));
        }
        assertEquals(expected2, re2);
    }

    @Test
    public void getK8sSecret() {
        String secretFilePath = CommonUtils.getK8sSecret(config);
        String expected = "/tmp/test.prop";
        assertEquals(expected, secretFilePath);
    }

    @Test
    public void trimDataset() {
        Dataset<Row> df = spark.createDataset(Arrays.asList("test", " test", " test "), Encoders.STRING()).toDF("msg");
        Dataset<Row> df2 = spark.createDataset(Arrays.asList("test", "test", "test"), Encoders.STRING()).toDF("msg");
        Dataset<Row> trimdf = CommonUtils.trimDataset(df, " ");
        assertDataFrameEquals(df2, trimdf);
    }

    @Test
    public void makeSourcesConfig() {

        Config conf = CommonUtils.makeSourcesConfig("test",
                "select * from test.test left join test.test_new on test.test.c1=test.test_new.c2",
                spark);
        String actual = "Config(SimpleConfigObject({\"source\":{\"cos0\":{\"table\":\"test.test_new\"," +
                "\"tag\":\"test\"},\"cos1\":{\"table\":\"test.test\",\"tag\":\"test\"}}}))";
        assertEquals(actual, conf.toString());
    }

    @Test
    public void getResultTableName() {
        Config config = ConfigFactory.parseString("{result_table_name=\"test.test\"}");
        String test = CommonUtils.getResultTableName(config, "test");
        assertEquals("test.test", test);
    }

    @Test
    public void getCommonConfig() {
        Config options = CommonUtils.getCommonConfig(config, "app.k8s.secrets.", false);
        String expected = "Config(SimpleConfigObject({\"dir\":\"/tmp\",\"file\":\"test.prop\"}))";
        assertEquals(expected, options.toString());
    }

    @Test
    public void extractFileNameWithoutExtension() {

        assertEquals("filename", CommonUtils.extractFileNameWithoutExtension("/test/filename.jpg"));
    }

    @Test
    public void setReaderOptions() throws IllegalAccessException {
        DataFrameReader dfr = spark.read().format("csv");
        Config config = ConfigFactory.parseString("{options.result_table_name=\"test.test\"}");
        CommonUtils.setReaderOptions(config, dfr);

        Field[] fields = DataFrameReader.class.getDeclaredFields();
        Field field = Arrays.stream(fields).filter(f -> {
            f.setAccessible(true);
            return f.getName().equalsIgnoreCase("extraOptions");
        }).findFirst().get();
        field.setAccessible(true);
        assertTrue(field.get(dfr).toString().contains("result_table_name -> test.test"));

    }

    @Test
    public void setWriterOptions() throws IllegalAccessException {
        DataFrameWriter<Long> dwr = spark.range(1).write();
        Config config = ConfigFactory.parseString("{options.result_table_name=\"test.test\"}");
        CommonUtils.setWriterOptions(config, dwr);

        Field[] fields = DataFrameWriter.class.getDeclaredFields();
        Field field = Arrays.stream(fields).filter(f -> {
            f.setAccessible(true);
            return f.getName().equalsIgnoreCase("extraOptions");
        }).findFirst().get();
        field.setAccessible(true);
        assertTrue(field.get(dwr).toString().contains("result_table_name -> test.test"));
    }


    @Test
    public void getUTCNow() {

        System.out.println(CommonUtils.getUTCNow());
    }


    @Test
    public void extractNumFromString() {

        int actual = CommonUtils.extractNumFromString("col(10)", 0);
        assertEquals(10, actual);

    }

    @Test
    public void prepareConfigFromCos() {
        System.setProperty("secret.dir","src/test/resources/application.conf");
        System.setProperty("config.file","cos://test/sparkdataflow/application.conf");
        CommonUtils.prepareConfigFromCos();
    }

    @Test
    public void trimEndNumber() {
        String str = "cos1";
        String str2 = "cos10";
        assertEquals("cos", CommonUtils.trimEndNumber(str));
        assertEquals("cos", CommonUtils.trimEndNumber(str2));
    }

    @Test
    public void getEnv() {
        System.setProperty("bigdata.environment", "");
        Config conf = configParser.getConfig();
        assertEquals("", CommonUtils.getEnv(conf));
    }

    @Test(expected = DataFlowException.class)
    public void httpGetRequest() {

        CommonUtils.httpGetRequest("http://www.g.g.com/");
        CommonUtils.httpGetRequest("http://www.baidu.com/");
    }

    @Test
    public void testLatestSchemaInRegistry(){

     String actual =    CommonUtils.latestSchemaInRegistry("sdf_avro_test","http://localhost:18081/");
     String expected ="{\"type\":\"record\",\"name\":\"test\",\"namespace\":\"test\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"}]}";
     assertEquals(expected, actual);

    }


    @Test
    public void batchProcess() {
        String JSON = "source {\n" +
                "\n" +
                "    dummy {\n" +
                "        limit=200\n" +
                "        result_table_name=test_dummy\n" +
                "    }\n" +
                "}\n" +
                "\n" +
                "\n" +
                "sink {\n" +
                "\n" +
                "     stdout {\n" +
                "            table=test_dummy\n" +
                "            limit=100\n" +
                "      }\n" +
                "\n" +
                "}";
        Config config = ConfigFactory.parseString(JSON);
        CommonUtils.batchProcess(spark, config);

    }



    @Test
    public void toScalaImmutableMap() {
        Map<String, String> map = new HashMap<>();
          map.put("test","test");

        scala.collection.immutable.Map scalamap = CommonUtils.toScalaImmutableMap(map);
        scala.collection.immutable.Map<String, String> targetMap = new scala.collection.immutable.HashMap<>();
        targetMap = targetMap.$plus(new scala.Tuple2<>("test","test"));
        assertEquals(targetMap, scalamap);
    }



    @Test
    public void testGetEnv() {
        Config conf = ConfigFactory.empty();
        System.setProperty("bigdata.environment", "vt");
        String env = CommonUtils.getEnv(conf);
        assertEquals("vt", env);
    }

    @Test
    public void testSetDataStreamWriterOptions() {
        Config config = ConfigFactory.parseString("{" +
                " options.part1=\"test\"\n" +
                " options.part2=2\n" +
                "}\n");
        DataStreamWriter<Row> dsw = spark.readStream().format("rate").load().writeStream();
        CommonUtils.setDataStreamWriterOptions(config, dsw);
    }

    @Test
    public void testSetDataStreamTrigger() {
        Config config = ConfigFactory.parseString("{" +
                " options.part1=\"test\"\n" +
                " options.part2=2\n" +
                " interval=\"1 second\"" +
                "}\n");
        DataStreamWriter<Row> dsw = spark.readStream().format("rate").load().writeStream();
        CommonUtils.setDataStreamTrigger(dsw, config, "OneTime");
        CommonUtils.setDataStreamTrigger(dsw, config, "Continuous");
    }
}