package com.zdjizhi.base.filters;


import com.zdjizhi.base.pojo.GrootEvent;
import com.zdjizhi.base.pojo.Rule;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Filter {

    SingleOutputStreamOperator<GrootEvent> filterFunction(SingleOutputStreamOperator<GrootEvent> singleOutputStreamOperator, Rule rule) throws Exception;
}
