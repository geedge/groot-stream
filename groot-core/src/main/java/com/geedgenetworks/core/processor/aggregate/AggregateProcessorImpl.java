package com.geedgenetworks.core.processor.aggregate;

import com.geedgenetworks.core.pojo.AggregateConfig;
import com.geedgenetworks.core.pojo.Event;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class AggregateProcessorImpl implements AggregateProcessor {

    @Override
    public SingleOutputStreamOperator<Event> aggregateProcessorFunction(
            SingleOutputStreamOperator<Event> singleOutputStreamOperator,
            AggregateConfig aggregateConfig)
            throws Exception {

        /*      List<String> keys = aggregateConfig.getGroup_by_fields();

            return singleOutputStreamOperator.keyBy(new OneKeySelector(keys)).window(TumblingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_interval_seconds()))).reduce(new ReduceFunction<Event>() {
                public Event reduce(Event v1, Event v2) {


                    Integer aa = (Integer) v1.getExtractedFields().get("common_sessions");
                    Integer bb = (Integer) v2.getExtractedFields().get("common_sessions");
                    v1.getExtractedFields().put("common_sessions",aa+bb);
                    return v1;
                }
            }).setParallelism(aggregateConfig.getParallelism());
        }*/
        return singleOutputStreamOperator;
    }
}
