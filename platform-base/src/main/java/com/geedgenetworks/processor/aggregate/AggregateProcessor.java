package com.geedgenetworks.processor.aggregate;

import com.geedgenetworks.pojo.Aggregate;
import com.geedgenetworks.pojo.GrootEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface AggregateProcessor {

    SingleOutputStreamOperator<GrootEvent> aggregateProcessorFunction(SingleOutputStreamOperator<GrootEvent> singleOutputStreamOperator, Aggregate aggregate) throws Exception;
}
