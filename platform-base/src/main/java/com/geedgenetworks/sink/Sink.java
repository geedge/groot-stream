package com.geedgenetworks.sink;

import com.geedgenetworks.pojo.GrootEvent;
import com.geedgenetworks.pojo.SinkConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Sink {


   void sink(SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator , SinkConfig sinkConfig) throws Exception;

}
