package com.geedgenetworks.processor.projection;

import com.geedgenetworks.pojo.GrootEvent;
import com.geedgenetworks.pojo.Projection;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface ProjectionProcessor {

    SingleOutputStreamOperator<GrootEvent> projectionProcessorFunction(SingleOutputStreamOperator<GrootEvent> grootEventSingleOutputStreamOperator, Projection projection) throws Exception;
}
