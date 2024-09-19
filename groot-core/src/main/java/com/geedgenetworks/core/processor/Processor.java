package com.geedgenetworks.core.processor;

import com.geedgenetworks.common.Event;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Processor<T> {

    DataStream<Event> processorFunction(
            DataStream<Event> singleOutputStreamOperator,
            T processorConfig, ExecutionConfig config)
            throws Exception;
    String type();
}
