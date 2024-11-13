package com.geedgenetworks.core.processor.filter;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.connector.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AviatorFilterProcessor implements Processor<FilterConfig> {

    @Override
    public DataStream<Event> process(StreamExecutionEnvironment env,
                                            DataStream<Event> input, FilterConfig FilterConfig) {
        SingleOutputStreamOperator<Event> resultStream  =
                input.filter(new FilterFunction(FilterConfig)).name(FilterConfig.getName());

        if (FilterConfig.getParallelism() != 0) {
            resultStream.setParallelism(FilterConfig.getParallelism());
        }
        return resultStream;
    }

    @Override
    public FilterConfig parseConfig(String name, Config config) {
        FilterConfig filterConfig = new JSONObject(config.root().unwrapped()).toJavaObject(FilterConfig.class);
        filterConfig.setName(name);
        return filterConfig;
    }

}
