package com.geedgenetworks.core.sink;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.SinkConfig;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Sink {

    void sink(
            SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator,
            SinkConfig sinkConfig)
            throws Exception;
}
