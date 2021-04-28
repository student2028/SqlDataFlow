package org.student.spark.api;

import com.typesafe.config.Config;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public interface Plugin extends Serializable {

    Logger logger = LoggerFactory.getLogger(Plugin.class);

    Map<String, HashMap<String,Object>> catalog = new ConcurrentHashMap<>();

    void setConfig(Config config);

    Config getConfig();

    boolean checkConfig();

    void prepare(SparkSession spark);

    void process(SparkSession spark);

    String getPluginName();

}
