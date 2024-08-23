package com.geedgenetworks.core.split;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.SplitConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Set;

public interface Split {

    DataStream<Event> splitFunction(
            DataStream<Event> dataStream, SplitConfig splitConfig)
            throws Exception;
    String type();
}
