package org.student.spark.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.storage.S3SingleDriverLogStore;
import scala.collection.Iterator;
import scala.collection.immutable.List;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class S3LogStore extends S3SingleDriverLogStore {

    private Configuration hadoopConf;
    private SparkConf sparkConf;

    public S3LogStore(SparkConf sparkConf, Configuration hadoopConf) {
        super(sparkConf, hadoopConf);
        this.hadoopConf = hadoopConf;
        this.sparkConf = sparkConf;
    }

    @Override
    public void write(Path path, Iterator<String> actions, boolean overwrite) {
        System.out.println(path);
        String tableNameRegex = ".*/(.*)/(?=_delta_log/)";
        String pathPrefixRegex = "(.*)(?=_delta_log/)";
        String pathBase = CommonUtils.regexExtractFirst(path.toString(), pathPrefixRegex);
        String table = CommonUtils.regexExtractFirst(path.toString(), tableNameRegex);

        List<String> list = actions.toList();
        super.write(path, list.iterator(), overwrite);
        try {
            java.util.List<Map<String, String>> resultList = new ArrayList<>();
            list.foreach(line -> {
                if (line.startsWith("{\"add")) {
                    Map<String, String> map = new HashMap<>();
                    map.put("topic", table);
                    String filePath = JsonUtils.serializer().jsonToMap(line.substring(7, line.length() - 1)).get("path").toString();
                    map.put("path", pathBase + filePath);
                    resultList.add(map);
                }
                return null;
            });
            SparkSession spark = SparkSession.active();
            Dataset<Row> df = spark.createDataset(Arrays.asList(JsonUtils.serializer().writeValueAsString(resultList)), Encoders.STRING()).toDF("value");
            //todo add kafkaParameters for auth
            String topics = sparkConf.get("delta.kafka.topic");
            String brokers = sparkConf.get("delta.kafka.brokers");
            final Map<String, String> kafkaParams = new HashMap<>();
             df.write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", brokers)
                    .option("topic", topics)
                    .options(kafkaParams)
                    .save();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void invalidateCache() {
    }

    @Override
    public boolean isPartialWriteVisible(Path path) {
        return false;
    }
}