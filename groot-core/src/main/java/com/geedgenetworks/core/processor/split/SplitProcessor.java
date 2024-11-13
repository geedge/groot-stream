package com.geedgenetworks.core.processor.split;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitProcessor implements Processor<SplitConfig> {

    @Override
    public DataStream<Event> process(StreamExecutionEnvironment env,
                                     DataStream<Event> input, SplitConfig splitConfig) {

        SingleOutputStreamOperator<Event> resultStream  =
                input.process(new SplitFunction(splitConfig)).name(splitConfig.getName());

        if (splitConfig.getParallelism() != 0) {
            resultStream.setParallelism(splitConfig.getParallelism());
        }
        return resultStream;
    }

    @Override
    public SplitConfig parseConfig(String name, Config config) {
        SplitConfig splitConfig = new JSONObject(config.root().unwrapped()).toJavaObject(SplitConfig.class);
        splitConfig.setName(name);
        return splitConfig;
    }

}
