package com.geedgenetworks.processor.projection;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.Projection;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface ProjectionProcessor {

    SingleOutputStreamOperator<Event> projectionProcessorFunction(SingleOutputStreamOperator<Event> grootEventSingleOutputStreamOperator, Projection projection) throws Exception;
}
