package org.student.spark.api;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public abstract class BaseSink implements Plugin {

    protected Config config = ConfigFactory.empty();

    protected String table;  //source view name
    protected String format = "parquet"; //save which format
    protected String save_mode = "error"; //mode
    protected String path;  //path


    public String getTable() {
        return table;
    }
 }
