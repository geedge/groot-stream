package com.geedgenetworks.processor.aggregate;


import com.geedgenetworks.pojo.Aggregate;
import com.geedgenetworks.common.OneKeySelector;
import com.geedgenetworks.pojo.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public class AggregateProcessorImpl implements AggregateProcessor {


    @Override
    public SingleOutputStreamOperator<Event> aggregateProcessorFunction(SingleOutputStreamOperator<Event> singleOutputStreamOperator, Aggregate aggregate) throws Exception {


        List<String> keys = aggregate.getGroup_by_fields();

        return singleOutputStreamOperator.keyBy(new OneKeySelector(keys)).window(TumblingEventTimeWindows.of(Time.seconds(aggregate.getWindow_interval_seconds()))).reduce(new ReduceFunction<Event>() {
            public Event reduce(Event v1, Event v2) {


                Integer aa = (Integer) v1.getExtractedFields().get("common_sessions");
                Integer bb = (Integer) v2.getExtractedFields().get("common_sessions");
                v1.getExtractedFields().put("common_sessions",aa+bb);
                return v1;
            }
        }).setParallelism(aggregate.getParallelism());
    }
}
