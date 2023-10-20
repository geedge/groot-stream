package com.geedgenetworks.processor.aggregate;

import com.geedgenetworks.pojo.AggregateConfig;
import com.geedgenetworks.pojo.Event;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface AggregateProcessor {

    SingleOutputStreamOperator<Event> aggregateProcessorFunction(
            SingleOutputStreamOperator<Event> singleOutputStreamOperator,
            AggregateConfig aggregateConfig)
            throws Exception;
}
