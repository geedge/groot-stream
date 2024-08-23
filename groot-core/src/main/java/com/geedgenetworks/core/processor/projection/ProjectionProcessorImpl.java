package com.geedgenetworks.core.processor.projection;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.ProjectionConfig;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

public class ProjectionProcessorImpl implements ProjectionProcessor {


    @Override
    public DataStream<Event> processorFunction(DataStream<Event> grootEventDataStream, ProjectionConfig projectionConfig, ExecutionConfig config) throws Exception {
        if (projectionConfig.getParallelism() != 0) {
            return grootEventDataStream
                    .process(new ProjectionProcessFunction(projectionConfig))
                    .setParallelism(projectionConfig.getParallelism())
                    .name(projectionConfig.getName());
        } else {
            return grootEventDataStream
                    .process(new ProjectionProcessFunction(projectionConfig))
                    .name(projectionConfig.getName());
        }
    }
    @Override
    public String type() {
        return "projection";
    }
}
