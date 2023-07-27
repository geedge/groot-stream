package net.geedgenetworks.processor.aggregate;


import net.geedgenetworks.common.OneKeySelector;
import net.geedgenetworks.pojo.Aggregate;
import net.geedgenetworks.pojo.GrootEvent;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

public class AggregateProcessorImpl implements AggregateProcessor {


    @Override
    public SingleOutputStreamOperator<GrootEvent> aggregateProcessorFunction(SingleOutputStreamOperator<GrootEvent> singleOutputStreamOperator, Aggregate aggregate) throws Exception {


        List<String> keys = aggregate.getGroup_by_fields();

        return singleOutputStreamOperator.keyBy(new OneKeySelector(keys)).window(TumblingEventTimeWindows.of(Time.seconds(aggregate.getWindow_interval_seconds()))).reduce(new ReduceFunction<GrootEvent>() {
            public GrootEvent reduce(GrootEvent v1, GrootEvent v2) {


                Integer aa = (Integer) v1.getAuxiliaryFieldsAndValues().get("common_sessions");
                Integer bb = (Integer) v2.getAuxiliaryFieldsAndValues().get("common_sessions");
                v1.getAuxiliaryFieldsAndValues().put("common_sessions",aa+bb);
                return v1;
            }
        }).setParallelism(aggregate.getParallelism());
    }
}
