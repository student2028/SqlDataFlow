package org.student.spark.source;

import org.student.spark.api.BaseSource;
import org.student.spark.common.CommonUtils;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/*
 *
 * Just used for test
 */
public class Dummy extends BaseSource {


    private long limit;

    @Override
    public String getPluginName() {
        String defaultName = String.format("%s_%s", getClass().getCanonicalName(), result_table_name);
        return CommonUtils.getStringWithDefault(config, "name", defaultName);
    }


    @Override
    public void process(SparkSession spark) {

        List<StructField> fields = new ArrayList<>();
        StructField field = DataTypes.createStructField("ID", DataTypes.LongType, false);
        fields.add(field);
        field = DataTypes.createStructField("COL_INT", DataTypes.IntegerType, false);
        fields.add(field);
        field = DataTypes.createStructField("NAME", DataTypes.StringType, false);
        fields.add(field);
        field = DataTypes.createStructField("COL_CHAR", DataTypes.StringType, false);
        fields.add(field);
        field = DataTypes.createStructField("COL_DOUBLE", DataTypes.DoubleType, false);
        fields.add(field);
        field = DataTypes.createStructField("COL_FLOAT", DataTypes.FloatType, false);
        fields.add(field);
        field = DataTypes.createStructField("COL_SHORT", DataTypes.ShortType, false);
        fields.add(field);
        field = DataTypes.createStructField("ADD_DATE", DataTypes.DateType, false);
        fields.add(field);
        field = DataTypes.createStructField("ADD_TS", DataTypes.TimestampType, false);
        fields.add(field);
        field = DataTypes.createStructField("COL_DECIMAL", DataTypes.createDecimalType(20, 3), false);
        fields.add(field);
        field = DataTypes.createStructField("COL_BOOLEAN", DataTypes.BooleanType, false);
        fields.add(field);
        field = DataTypes.createStructField("COL_BYTE", DataTypes.ByteType, false);
        fields.add(field);
        StructType schema = DataTypes.createStructType(fields);

        //limit
        Random random = new Random();
        List<Row> rows = new ArrayList<>();
        for (long i = 0; i < limit; i++) {
            Row row = RowFactory.create(i,
                    Math.toIntExact(i),
                    "Name" + i,
                    "CHAR" + i,
                    random.nextDouble(),
                    random.nextFloat(),
                    Short.parseShort(String.valueOf(random.nextInt(128))),
                    java.sql.Date.valueOf("2020-01-01"),
                    java.sql.Timestamp.valueOf(LocalDateTime.now()),
                    BigDecimal.valueOf(Math.random()).multiply(new BigDecimal(999999999)).setScale(20, 3),
                    i % 2 == 1,
                    Integer.valueOf(random.nextInt(100)).byteValue()
             );
            rows.add(row);
        }

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.createOrReplaceTempView(result_table_name);
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

    @Override
    public boolean checkConfig() {
        return false;
    }

    @Override
    public void prepare(SparkSession spark) {

        limit = CommonUtils.getLongWithDefault(config, "limit", 100);
        result_table_name = CommonUtils.getStringWithDefault(config, "result_table_name", "dummy");

    }
}
