package com.geedgenetworks.core.split;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.spi.split.Split;
import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.split.SplitConfig;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Map;

public class SplitOperator implements Split<SplitConfig> {

    @Override
    public DataStream<Event> splitFunction(
            DataStream<Event> dataStream, SplitConfig splitConfig)
            throws Exception {
        if (splitConfig.getParallelism() != 0) {
            return dataStream
                    .process(new SplitFunction(splitConfig))
                    .setParallelism(splitConfig.getParallelism())
                    .name(splitConfig.getName());
        } else {
            return dataStream
                    .process(new SplitFunction(splitConfig))
                    .name(splitConfig.getName());
        }
    }
    @Override
    public String type() {
        return "split";
    }

    @Override
    public SplitConfig checkConfig(String name, Map<String, Object> configProperties, Config config) {
        SplitConfig splitConfig = new JSONObject(configProperties).toJavaObject(SplitConfig.class);
        splitConfig.setName(name);
        return splitConfig;
    }

}
