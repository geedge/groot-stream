package com.geedgenetworks.core.processor.projection;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.ProjectionConfig;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class ProjectionProcessorImpl implements ProjectionProcessor {

    @Override
    public SingleOutputStreamOperator<Event> projectionProcessorFunction(
            SingleOutputStreamOperator<Event> grootEventSingleOutputStreamOperator,
            ProjectionConfig projectionConfig)
            throws Exception {

        if (projectionConfig.getParallelism() != 0) {
            return grootEventSingleOutputStreamOperator
                    .process(new ProjectionProcessFunction(projectionConfig))
                    .setParallelism(projectionConfig.getParallelism())
                    .name(projectionConfig.getName());
        } else {
            return grootEventSingleOutputStreamOperator
                    .process(new ProjectionProcessFunction(projectionConfig))
                    .name(projectionConfig.getName());
        }
    }
}
