package org.student.spark;

import com.typesafe.config.*;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.student.spark.api.*;
import org.student.spark.common.CommonUtils;
import org.student.spark.common.CosUtils;
import org.student.spark.processor.Sql;

import java.io.File;
import java.util.*;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.student.spark.common.CommonUtils.extractTables;


public class ConfigParser {
    private static Config appConfig;
    private final static String SOURCE = "source";
    private final static String SOURCE_DOT = "source.";

    final static Logger logger = LoggerFactory.getLogger(ConfigParser.class);

    static {
        appConfig = ConfigFactory.load();
        appConfig = CommonUtils.mergeK8sSecrets(appConfig);
    }

    private Config config = ConfigFactory.empty();

    public ConfigParser(Config _config) {
        this.config = _config;
        config = config.withFallback(appConfig);
    }

    public ConfigParser() {
        config = config.withFallback(appConfig);
    }

    public ConfigParser(String configFilePath) {
        this.config = Load(configFilePath);
    }

    public void config(String key, Object value) {
        config = config.withValue(key, ConfigValueFactory.fromAnyRef(value));
    }


    public ConfigParser loadString(String configContent) {
        config = ConfigFactory
                .parseString(configContent)
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
        return this;
    }

    private Config Load(String configFile) {
        Config config;
        if (configFile.startsWith("cos://")) {
            String configContent = CosUtils.getFileAsString(configFile);
            config = ConfigFactory
                    .parseString(configContent)
                    .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
        } else {
            config = ConfigFactory
                    .parseFile(new File(configFile))
                    .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
        }

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        logger.info("parsed config file: " + config.root().render(options));
        config = config.withFallback(appConfig);
        return config;
    }

    public boolean checkConfig(String configFile) {
        try {
            ConfigFactory
                    .parseFile(new File(configFile))
                    .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
            return true;
        } catch (Exception ex) {
            logger.warn(ex.getMessage());
            return false;
        }
    }

    public Config getSparkConfig() {
        return config.getConfig("spark");
    }

    public Config getConfig() {
        return config;
    }

    public List<BaseSource> createSources() {
        List<BaseSource> sourceList = new ArrayList<>();
        if (!config.hasPath(SOURCE)) {
            return sourceList;
        }

        return config.getConfig("source").root().keySet().stream()
                .sorted(Comparator.comparing(CommonUtils::extractNumber))
                .map(plugin -> {
                    String key = "classmap.source." + CommonUtils.trimEndNumber(plugin);
                    if (appConfig.hasPath(key)) {
                        logger.info(appConfig.getString(key));
                        try {
                            Config conf = config.getConfig("source." + plugin)
                                    .withFallback(config).withFallback(appConfig);
                            if (!CommonUtils.getBooleanWithDefault(conf, "enabled", true)) {
                                return null;
                            }
                            BaseSource source = (BaseSource) Class.forName(appConfig.getString(key))
                                    .newInstance();
                            source.setConfig(conf);
                            return source;
                        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                            logger.error("create sources", e);
                        }
                    } else {
                        logger.info("not found class source." + plugin);
                    }
                    return null;
                }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    public List<BaseProcessor> createProcessors() {
        if (!config.hasPath("processor")) {
            return new ArrayList<>();
        }

        return config.getConfig("processor").root().keySet().stream()
                .sorted(Comparator.comparing(CommonUtils::extractNumber))
                .map(plugin -> {
                    String key = "classmap.processor." + CommonUtils.trimEndNumber(plugin);
                    if (appConfig.hasPath(key)) {
                        logger.info(appConfig.getString(key));
                        try {
                            Config conf = config.getConfig("processor." + plugin)
                                    .withFallback(config).withFallback(appConfig);
                            if (!CommonUtils.getBooleanWithDefault(conf, "enabled", true)) {
                                return null;
                            }
                            BaseProcessor processor = (BaseProcessor) Class.forName(appConfig.getString(key))
                                    .newInstance();
                            processor.setConfig(conf);
                            return processor;
                        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                            logger.error("createProssors", e);
                        }
                    } else {
                        logger.info("not found class processor." + plugin);
                    }
                    return null;
                }).filter(Objects::nonNull).collect(Collectors.toList());
    }


    public List<BaseSink> createSinks() {
        List<BaseSink> sinkList = new ArrayList<>();
        if (!config.hasPath("sink")) {
            return sinkList;
        }

        sinkList = config.getConfig("sink").root().keySet().stream()
                .sorted(Comparator.comparing(CommonUtils::extractNumber))
                .map(plugin -> {
                    String key = "classmap.sink." + CommonUtils.trimEndNumber(plugin);
                    if (appConfig.hasPath(key)) {
                        logger.info(appConfig.getString(key));
                        try {
                            Config conf = config.getConfig("sink." + plugin)
                                    .withFallback(config).withFallback(appConfig);
                            if (!CommonUtils.getBooleanWithDefault(conf, "enabled", true)) {
                                return null;
                            }
                            BaseSink sink = (BaseSink) Class.forName(appConfig.getString(key))
                                    .newInstance();
                            sink.setConfig(conf);
                            return sink;
                        } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
                            logger.error("createSinks", e);
                        }
                    } else {
                        logger.info("not found class sink." + plugin);
                    }
                    return null;
                }).filter(Objects::nonNull).collect(Collectors.toList());

        return sinkList;
    }

    public static List<BaseSource> createSources(Config config) {
        List<BaseSource> sourceList = new ArrayList<>();
        if (!config.hasPath(SOURCE)) {
            return sourceList;
        }

        sourceList = config.getConfig(SOURCE).root().keySet().stream()
                .map(plugin -> createSource(config, plugin)).filter(Objects::nonNull).collect(Collectors.toList());

        return sourceList;
    }

    private static BaseSource createSource(Config config, String plugin) {
        String key = "classmap.source." + CommonUtils.trimEndNumber(plugin);
        if (appConfig.hasPath(key)) {
            logger.info(appConfig.getString(key));
            try {
                BaseSource source = (BaseSource) Class.forName(appConfig.getString(key)).newInstance();
                Config conf = config.getConfig(SOURCE_DOT + plugin).withFallback(config).withFallback(appConfig);
                source.setConfig(conf);
                return source;
            } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
                logger.error("createSource", e);
            }
        } else {
            logger.info("not found class " + SOURCE_DOT + plugin);
        }
        return null;
    }

    public List<Pipeline> getPipelines() {
        List<Pipeline> pipelineList = new ArrayList<>();
        List<BaseSource> sourceList = createSources();
        List<BaseProcessor> processorList = createProcessors();
        List<BaseSink> sinkList = createSinks();
        //do prepare in main Thread , find config error before running
        SparkSession spark = SparkSession.active();
        for (Plugin plugin : sourceList) {
            plugin.prepare(spark);
        }
        for (Plugin plugin : processorList) {
            plugin.prepare(spark);
        }
        for (Plugin plugin : sinkList) {
            plugin.prepare(spark);
        }
        /**
         *  build pipeline from sink, use sink's table attribute to filter processor's viewName, if not found, to filter sourceList 's result_table_name attribute
         *  if found in processList, recursively find in processorList , take care of sql , sql not use table attribute ,so we should extract table name from sql string
         *  now just consider the case without common processor or source when make pipeline
         *   source1 source2 ...-> processor1 -> processor2 -> processor .... -> sink1
         *   source3 -> processor4 -> .... sink2
         *
         *  write at 2021-03-26
         */
        sinkList.forEach(sink -> {
            Pipeline pipeline = new Pipeline();
            pipeline.setSink(sink);
            Optional<BaseProcessor> processor = processorList.stream().filter(p -> p.getResult_table_name().equalsIgnoreCase(sink.getTable())).findFirst();
            if (processor.isPresent()) {
                pipeline.addProcessor(processor.get());
                if (processor.get() instanceof Sql) {
                    Sql sql = (Sql) processor.get();
                    Set<String> tables = extractTables(sql.getSql(), spark);
                    for (String table : tables) {
                        Optional<BaseSource> baseSource = sourceList.stream().filter(s -> s.getResultTableName().equalsIgnoreCase(table)).findFirst();
                        if (baseSource.isPresent()) {
                            pipeline.addSource(baseSource.get());
                        } else { // processor
                            Optional<BaseProcessor> temp = processorList.stream().filter(p -> p.getResult_table_name().equalsIgnoreCase(table)).findFirst();
                            if (temp.isPresent()) {
                                pipeline.addProcessor(temp.get());
                                //if the new process is sql or not?
                                while (temp != null && temp.isPresent()) {
                                    if (temp.get() instanceof Sql) {
                                        sql = (Sql) temp.get();
                                        Set<String> tablesTemp = extractTables(sql.getSql(), spark);
                                        for (String tableTemp : tablesTemp) {
                                            baseSource = sourceList.stream().filter(s -> s.getResultTableName().equalsIgnoreCase(tableTemp)).findFirst();
                                            if (baseSource.isPresent()) {
                                                pipeline.addSource(baseSource.get());
                                                temp = null;
                                            } else { //maybe also sql processor
                                                temp = processorList.stream().filter(p -> p.getResult_table_name().equalsIgnoreCase(tableTemp)).findFirst();
                                                pipeline.addProcessor(temp.get());
                                            }
                                        }
                                    } // not sql
                                    else {
                                        Optional<BaseProcessor> finalTemp = temp;
                                        temp = processorList.stream().filter(p -> p.getResult_table_name().equalsIgnoreCase(finalTemp.get().getResult_table_name())).findFirst();
                                        pipeline.addProcessor(temp.get());
                                    }
                                }
                            }
                        }
                    }
                } else {
                    Optional<BaseSource> baseSource = sourceList.stream().filter(s -> s.getResultTableName().equalsIgnoreCase(processor.get().getTable())).findFirst();
                    if (baseSource.isPresent()) {
                        pipeline.addSource(baseSource.get());
                    }
                }
            } else { //no processor
                Optional<BaseSource> baseSource = sourceList.stream().filter(s -> s.getResultTableName().equalsIgnoreCase(sink.getTable())).findFirst();
                if (baseSource.isPresent()) {
                    pipeline.addSource(baseSource.get());
                }
            }
            pipelineList.add(pipeline);
        });

        return pipelineList;
    }


}
