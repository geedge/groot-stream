package com.geedgenetworks.sink;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.SinkConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Sink {


   void sink(SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator , SinkConfig sinkConfig) throws Exception;

}
