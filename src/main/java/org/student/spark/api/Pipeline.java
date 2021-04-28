package org.student.spark.api;

import org.apache.spark.sql.SparkSession;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class Pipeline {

    List<BaseSource> sourceList = new ArrayList<>();
    BaseSink sink;

    public BaseSink getSink() {
        return sink;
    }

    public void setSink(BaseSink sink) {
        this.sink = sink;
    }

    public void addProcessor(BaseProcessor processor) {
        processorList.push(processor);
    }

    public void addSource(BaseSource source) {
        sourceList.add(source);
    }

    //not use java original stack
    Deque<BaseProcessor> processorList = new ArrayDeque<>();

    public void start(SparkSession spark) {
        sourceList.forEach(s->s.process(spark));
        processorList.stream().forEach(p-> p.process(spark));
        sink.process(spark);
    }

}
