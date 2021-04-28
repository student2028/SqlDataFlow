package org.student.spark;

import org.student.spark.common.CommonUtils;
import org.student.spark.common.CosUtils;
import org.student.spark.common.JdbcUtils;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3URI;
import io.findify.s3mock.S3Mock;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKWithSR;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafka;
import net.manub.embeddedkafka.schemaregistry.EmbeddedKafkaConfig;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import za.co.absa.abris.avro.read.confluent.SchemaManager;
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory;
import za.co.absa.abris.avro.registry.SchemaSubject;
import za.co.absa.abris.config.AbrisConfig;
import za.co.absa.abris.config.ToAvroConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.avro.functions.to_avro;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static za.co.absa.abris.avro.functions.to_avro;

public class SparkTestBase {

    protected final static Logger logger = LoggerFactory.getLogger(SparkTestBase.class);
    protected static AmazonS3URI uri;
    protected static String bucketFile;

    protected static EmbeddedPostgres pg;
    protected static EmbeddedKWithSR esr;

    public SparkSession getSpark() {
        return spark;
    }


    protected static SparkSession spark;

    protected static ConfigParser configParser;

    protected static SparkSession createSparkSession() {
        //System.setProperty("bigdata.environment", "vt");
        configParser = new ConfigParser();
        SparkConf sparkConf = CommonUtils.createSparkConf(configParser);
        System.out.println(sparkConf.toDebugString());
        sparkConf.set("spark.master", "local");
        sparkConf.set("spark.sql.shuffle.partitions", "5");
        sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
        spark = SparkSession.builder().appName("sparkdataflow_test").config(sparkConf).getOrCreate();
        String level = CommonUtils.getStringWithDefault(configParser.getConfig(), "spark.log.level", "info");
        spark.sparkContext().setLogLevel(level);

        return spark;
    }

    protected static SparkSession createSparkSession(String configFilePath) {
        configParser = new ConfigParser(configFilePath);
        SparkConf sparkConf = CommonUtils.createSparkConf(configParser);
        spark = SparkSession.builder().config(sparkConf).getOrCreate();
        return spark;
    }

    @BeforeClass
    public synchronized static void setUp() throws IOException {
        if (spark == null) {
            createSparkSession();

            if (!spark.conf().contains("spark.test.useEmbedComponents") || spark.conf().get("spark.test.useEmbedComponents").equalsIgnoreCase("true"))
             {
                pg = EmbeddedPostgres.builder()
                        .setPort(5432)
                        .start();
                String sql = CommonUtils.loadFile("src/test/resources/sql/metainit.sql");
                JdbcUtils.executeSql(configParser.getConfig(), "test", sql);
                makeS3Mock();
                makeKafkaServer();
            }

        }
    }

//    @AfterClass
//    public static void tearDown() {
//        if (spark != null)
//            spark.stop();
//    }


    protected void batchProcess(SparkSession spark, ConfigParser _configParser) {

        _configParser.config("spark.streaming.test", true);
        CommonUtils.batchProcess(spark, _configParser);
    }

    public boolean assertDataFrameEquals(Dataset<Row> a, Dataset<Row> b) {

        try {
            a.rdd().cache();
            b.rdd().cache();
            // 1. Check the equality of two schemas
            if (!a.schema().toString().equalsIgnoreCase(b.schema().toString())) {
                System.out.println("schema is not same");
                System.out.println(a.schema().toString());
                System.out.println(b.schema().toString());
                return false;
            }
            // 2. Check the number of rows in two dfs
            if (a.count() != b.count()) {
                return false;
            }
            // 3. Check there is no unequal rows and column name ignore case
            List<String> aColumns = Arrays.stream(a.columns()).map(String::toUpperCase).collect(Collectors.toList());
            List<String> bColumns = Arrays.stream(b.columns()).map(String::toUpperCase).collect(Collectors.toList());
            // To correctly handles cases where the DataFrames may have columns in different orders
            Collections.sort(aColumns);
            Collections.sort(bColumns);

            Column[] aArray = new Column[aColumns.size()];
            aArray = aColumns.stream().map(functions::col).collect(Collectors.toList()).toArray(aArray);
            Column[] bArray = new Column[bColumns.size()];
            bArray = bColumns.stream().map(functions::col).collect(Collectors.toList()).toArray(bArray);

            Dataset<Row> a_prime;
            Dataset<Row> b_prime;

            // To correctly handles cases where the DataFrames may have duplicate rows
            // and/or rows in different orders
            a_prime = a.sort(aArray).groupBy(aArray).count();
            a_prime.show(false);
            b_prime = b.sort(bArray).groupBy(bArray).count();
            a_prime.show(false);

            Long c1 = a_prime.except(b_prime).count();
            a_prime.except(b_prime).show(false);
            Long c2 = b_prime.except(a_prime).count();
            b_prime.except(a_prime).show(false);

            if (!c1.equals(c2) || c1 != 0) {
                return false;
            }


        } finally {
            a.rdd().unpersist(true);
            b.rdd().unpersist(true);
        }

        return true;
    }


    public Dataset<Row> createDataFrame(List<List<Object>> values, StructType schema) {
        List<Row> rows = new ArrayList<>();

        for (List<Object> value : values) {
            rows.add(RowFactory.create(value.toArray()));
        }

        return spark.createDataFrame(rows, schema);
    }


    public StructType createStructType(String schemaString) {

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        return DataTypes.createStructType(fields);

    }

    protected Dataset<Row> createKVDataSet(
            List<Tuple2<Integer, Integer>> data, String keyName, String valueName) {
        Encoder<Tuple2<Integer, Integer>> encoder = Encoders.tuple(Encoders.INT(), Encoders.INT());
        return spark.createDataset(data, encoder).toDF(keyName, valueName);
    }

    protected Dataset<Row> createKVDataSet(List<Tuple2<Integer, Integer>> data) {
        Encoder<Tuple2<Integer, Integer>> encoder = Encoders.tuple(Encoders.INT(), Encoders.INT());
        return spark.createDataset(data, encoder).toDF();
    }

    protected <T1, T2> Tuple2<T1, T2> tuple2(T1 t1, T2 t2) {
        return new Tuple2<>(t1, t2);
    }

    protected static void makeS3Mock() {

        S3Mock api = new S3Mock.Builder().withPort(9091).withInMemoryBackend().build();
        api.start();
        CosUtils.createS3Client("test", "test", "localhost:9091", true);
        CosUtils.getS3Client().createBucket("test");

        uri = new AmazonS3URI("s3://test/sparkdataflow/application.conf");
        bucketFile = "s3://test/sparkdataflow/application.conf";
        CosUtils.putObject("test", "sparkdataflow/application.conf",
                "src/main/resources/application.conf",
                "sparkdataflow configuration file");

        CosUtils.putObject("test", "test.test1/20201013001411041/test.gz.parquet",
                "src/test/resources/test-data/parquet/test/part-00000-244bfb37-8dca-4371-8afd-a7318e178af7-c000.gz.parquet",
                "sparkdataflow test.test file");

        CosUtils.putObject("test", "sparkdataflow/dataflow/test.conf",
                "src/test/resources/dataflow/examples/dummy2jdbc.conf", "just for test demo dataflow");
    }

    protected static void makeKafkaServer() {
        if (null == esr) {
            scala.collection.immutable.Map<String, String> map = new scala.collection.immutable.HashMap<>();
            EmbeddedKafkaConfig config = EmbeddedKafkaConfig.apply(
                    9083,
                    7001,
                    18081,
                    map,
                    map,
                    map,
                    map
            );
            esr = EmbeddedKafka.start(config);
            //initial one schema info
            scala.collection.immutable.Map<String, String> srConfig = new scala.collection.immutable.HashMap<>();
            srConfig = srConfig.$plus(new scala.Tuple2<>("schema.registry.url", "http://localhost:18081"));
            SchemaManager schemaManager = SchemaManagerFactory.create(srConfig);

            String schemaString = "{\"type\":\"record\",\"name\":\"test\",\"namespace\":\"test\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}" +
                    ",{\"name\":\"name\",\"type\":\"string\"}]}";
            SchemaSubject subject = SchemaSubject.usingTopicNameStrategy("sdf_avro_test", false);
            schemaManager.register(subject, schemaString);

            //add one topic data
            Dataset<Row> df = spark.range(1, 100).map((MapFunction<Long, String>) i
                    ->
                    String.valueOf(i), Encoders.STRING()).toDF("value");

            df.write().format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9083")
                    .option("topic", "sdf_plain_test")
                    .save();

            Dataset<Row> df2 = spark.range(1, 10).map(
                    (MapFunction<Long, Tuple2<Long, String>>) i ->
                            new Tuple2<>(i, String.valueOf(i) + "测试数据student"),
                    Encoders.tuple(Encoders.LONG(), Encoders.STRING())
            ).toDF("id", "name");
            Dataset<Row> dfavro = df2.select(to_avro(struct(col("*"))).as("value"));
            dfavro.write().format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9083")
                    .option("topic", "sdf_spark_avro_test")
                    .save();

            ToAvroConfig toAvroConfig = AbrisConfig
                    .toConfluentAvro()
                    .downloadSchemaByLatestVersion()
                    .andTopicNameStrategy("sdf_avro_test", false)
                    .usingSchemaRegistry("http://localhost:18081/");

            Dataset<Row> dfConfluentAvro = df2.select(to_avro(struct(col("*")), toAvroConfig).alias("value"));

            dfConfluentAvro.write().format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9083")
                    .option("topic", "sdf_avro_test")
                    .save();

        }
    }

    protected void makeStreamSource(String id, String name, String tableName) {
        Dataset<Row> df = spark.readStream().format("rate")
                .option("numPartitions", 1)
                .option("rowsPerSecond", 2)
                .load()
                .selectExpr(id + " as id", String.format("'%s' as name", name));
        df.createOrReplaceTempView(tableName);
    }

    protected String getCheckpointDir(String tableName) {
        String checkpointDir = System.getProperty("java.io.tmpdir") + "checkpoints/" + tableName;
        java.io.File checkpointFile = new java.io.File(checkpointDir);
        if (checkpointFile.exists()) {
            try {
                FileUtils.deleteDirectory(checkpointFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return checkpointDir;
    }
}
