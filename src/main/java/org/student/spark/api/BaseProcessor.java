package org.student.spark.api;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public abstract class BaseProcessor implements Plugin {

    protected Config config = ConfigFactory.empty();
    protected String table;
    protected String result_table_name;

    public String getResult_table_name() {
        return this.result_table_name;
    }

    public String getTable() {
        return this.table;
    }
}
