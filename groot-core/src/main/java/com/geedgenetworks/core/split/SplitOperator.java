package com.geedgenetworks.core.split;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.SplitConfig;
import org.apache.flink.streaming.api.datastream.DataStream;

public class SplitOperator implements Split {

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

}
