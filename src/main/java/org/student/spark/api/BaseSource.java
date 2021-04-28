package org.student.spark.api;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public abstract class BaseSource implements Plugin {

    protected Config config = ConfigFactory.empty();

    protected String result_table_name;

    public Dataset<Row> getDataset(SparkSession spark) {
        if (spark.catalog().tableExists(result_table_name)) {
            return spark.table(result_table_name);
        } else {return null;}
    }

    public String getResultTableName() {
        return result_table_name;
    }

}
