package com.geedgenetworks.core.processor.aggregate;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.AggregateConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.geedgenetworks.common.Constants.*;

public class AggregateProcessorImpl implements AggregateProcessor {

    @Override
    public SingleOutputStreamOperator<Event> aggregateProcessorFunction(
            SingleOutputStreamOperator<Event> grootEventSingleOutputStreamOperator,
            AggregateConfig aggregateConfig, ExecutionConfig config)
            throws Exception {

        if (aggregateConfig.getParallelism() != 0) {
            switch (aggregateConfig.getWindow_type()) {
                case TUMBLING_PROCESSING_TIME:
                    return grootEventSingleOutputStreamOperator.keyBy(new KeySelector(aggregateConfig.getGroup_by_fields())).window(TumblingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()))).aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl()).setParallelism(aggregateConfig.getParallelism()).name(aggregateConfig.getName());
                case TUMBLING_EVENT_TIME:
                    return grootEventSingleOutputStreamOperator.keyBy(new KeySelector(aggregateConfig.getGroup_by_fields())).window(TumblingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()))).aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl()).setParallelism(aggregateConfig.getParallelism()).name(aggregateConfig.getName());
                case SLIDING_PROCESSING_TIME:
                    return grootEventSingleOutputStreamOperator.keyBy(new KeySelector(aggregateConfig.getGroup_by_fields())).window(SlidingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide()))).aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl()).setParallelism(aggregateConfig.getParallelism()).name(aggregateConfig.getName());
                case SLIDING_EVENT_TIME:
                    return grootEventSingleOutputStreamOperator.keyBy(new KeySelector(aggregateConfig.getGroup_by_fields())).window(SlidingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide()))).aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl()).setParallelism(aggregateConfig.getParallelism()).name(aggregateConfig.getName());
                default:
                    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Invalid window type");
            }
        }else {
            switch (aggregateConfig.getWindow_type()) {
                case TUMBLING_PROCESSING_TIME:
                    return grootEventSingleOutputStreamOperator.keyBy(new KeySelector(aggregateConfig.getGroup_by_fields())).window(TumblingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()))).aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl()).name(aggregateConfig.getName());
                case TUMBLING_EVENT_TIME:
                    return grootEventSingleOutputStreamOperator.keyBy(new KeySelector(aggregateConfig.getGroup_by_fields())).window(TumblingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()))).aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl()).name(aggregateConfig.getName());
                case SLIDING_PROCESSING_TIME:
                    return grootEventSingleOutputStreamOperator.keyBy(new KeySelector(aggregateConfig.getGroup_by_fields())).window(SlidingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide()))).aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl()).name(aggregateConfig.getName());
                case SLIDING_EVENT_TIME:
                    return grootEventSingleOutputStreamOperator.keyBy(new KeySelector(aggregateConfig.getGroup_by_fields())).window(SlidingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide()))).aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl()).name(aggregateConfig.getName());
                default:
                    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Invalid window type");
            }
        }
    }

    @Override
    public String type() {
        return "aggregate";
    }
}
