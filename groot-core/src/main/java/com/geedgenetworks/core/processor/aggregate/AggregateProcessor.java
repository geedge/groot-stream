package com.geedgenetworks.core.processor.aggregate;

import com.geedgenetworks.core.pojo.AggregateConfig;
import com.geedgenetworks.core.pojo.Event;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface AggregateProcessor {

    SingleOutputStreamOperator<Event> aggregateProcessorFunction(
            SingleOutputStreamOperator<Event> singleOutputStreamOperator,
            AggregateConfig aggregateConfig)
            throws Exception;
}
