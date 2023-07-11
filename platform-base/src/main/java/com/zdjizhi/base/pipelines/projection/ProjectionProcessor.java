package com.zdjizhi.base.pipelines.projection;

import com.zdjizhi.base.pojo.GrootEvent;
import com.zdjizhi.base.pojo.Projection;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface ProjectionProcessor {

    SingleOutputStreamOperator<GrootEvent> projectionProcessorFunction(SingleOutputStreamOperator<GrootEvent> grootEventSingleOutputStreamOperator, Projection projection) throws Exception;
}
