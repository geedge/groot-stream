package net.geedgenetworks.sink;

import net.geedgenetworks.pojo.GrootEvent;
import net.geedgenetworks.pojo.SinkConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Sink {


   void sink(SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator , SinkConfig sinkConfig) throws Exception;

}
