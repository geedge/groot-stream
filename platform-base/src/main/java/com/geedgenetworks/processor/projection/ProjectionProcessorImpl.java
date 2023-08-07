package com.geedgenetworks.processor.projection;


import com.geedgenetworks.pojo.GrootEvent;
import com.geedgenetworks.pojo.Projection;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class ProjectionProcessorImpl implements ProjectionProcessor {






    @Override
    public SingleOutputStreamOperator<GrootEvent> projectionProcessorFunction(SingleOutputStreamOperator<GrootEvent> grootEventSingleOutputStreamOperator, Projection projection) throws Exception {
        return grootEventSingleOutputStreamOperator.process(new ProjectionProcessFunction(projection)).setParallelism(projection.getParallelism()).name(projection.getName());
    }
}
