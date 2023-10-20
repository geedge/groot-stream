package com.geedgenetworks.processor.projection;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionConfig;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface ProjectionProcessor {

    SingleOutputStreamOperator<Event> projectionProcessorFunction(
            SingleOutputStreamOperator<Event> grootEventSingleOutputStreamOperator,
            ProjectionConfig projectionConfig)
            throws Exception;
}
