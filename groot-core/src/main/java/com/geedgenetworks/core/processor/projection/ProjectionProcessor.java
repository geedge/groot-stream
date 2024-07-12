package com.geedgenetworks.core.processor.projection;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.ProjectionConfig;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface ProjectionProcessor {

    SingleOutputStreamOperator<Event> projectionProcessorFunction(
            SingleOutputStreamOperator<Event> grootEventSingleOutputStreamOperator,
            ProjectionConfig projectionConfig)
            throws Exception;
    String type();
}
