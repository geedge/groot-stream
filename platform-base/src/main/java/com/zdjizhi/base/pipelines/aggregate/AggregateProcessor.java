package com.zdjizhi.base.pipelines.aggregate;

import com.zdjizhi.base.pojo.Aggregate;
import com.zdjizhi.base.pojo.GrootEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface AggregateProcessor {

    SingleOutputStreamOperator<GrootEvent> aggregateProcessorFunction(SingleOutputStreamOperator<GrootEvent> singleOutputStreamOperator, Aggregate aggregate) throws Exception;
}
