package com.zdjizhi.base.sinks;

import com.zdjizhi.base.pojo.GrootEvent;
import com.zdjizhi.base.pojo.SinkConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Sink {


   void sink(SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator , SinkConfig sinkConfig) throws Exception;

}
