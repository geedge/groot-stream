package com.geedgenetworks.core.processor.aggregate;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.processor.Processor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

import static com.geedgenetworks.common.config.Constants.*;

public class AggregateProcessor implements Processor<AggregateConfig> {

    @Override
    public DataStream<Event> process(StreamExecutionEnvironment env, DataStream<Event> input, AggregateConfig aggregateConfig) {

        SingleOutputStreamOperator<Event> singleOutputStreamOperator;
        if (aggregateConfig.getMini_batch()) {
            switch (aggregateConfig.getWindow_type()) {
                case TUMBLING_PROCESSING_TIME:
                    singleOutputStreamOperator = input
                            .process(new FirstAggregationProcessingTime(aggregateConfig, aggregateConfig.getWindow_size()))
                            .keyBy(new PreKeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(TumblingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size())))
                            .aggregate(new SecondAggregateProcessorFunction(env, aggregateConfig), new ProcessWindowFunction(aggregateConfig));
                    break;
                case TUMBLING_EVENT_TIME:
                    singleOutputStreamOperator = input
                            .process(new FirstAggregationEventTime(aggregateConfig, aggregateConfig.getWindow_size()))
                            .keyBy(new PreKeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(TumblingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size())))
                            .aggregate(new SecondAggregateProcessorFunction(env, aggregateConfig), new ProcessWindowFunction(aggregateConfig));
                    break;
                case SLIDING_PROCESSING_TIME:
                    singleOutputStreamOperator = input
                            .process(new FirstAggregationProcessingTime(aggregateConfig, aggregateConfig.getWindow_slide()))
                            .keyBy(new PreKeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(SlidingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide())))
                            .aggregate(new SecondAggregateProcessorFunction(env, aggregateConfig), new ProcessWindowFunction(aggregateConfig));
                    break;
                case SLIDING_EVENT_TIME:
                    singleOutputStreamOperator = input
                            .process(new FirstAggregationEventTime(aggregateConfig, aggregateConfig.getWindow_slide()))
                            .keyBy(new PreKeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(SlidingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide())))
                            .aggregate(new SecondAggregateProcessorFunction(env, aggregateConfig), new ProcessWindowFunction(aggregateConfig));
                    break;
                default:
                    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Invalid window type");

            }
        } else {
            switch (aggregateConfig.getWindow_type()) {
                case TUMBLING_PROCESSING_TIME:
                    singleOutputStreamOperator = input
                            .keyBy(new KeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(TumblingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size())))
                            .aggregate(new AggregateProcessorFunction(env, aggregateConfig), new ProcessWindowFunction(aggregateConfig));
                    break;
                case TUMBLING_EVENT_TIME:
                    singleOutputStreamOperator = input
                            .keyBy(new KeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(TumblingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size())))
                            .aggregate(new AggregateProcessorFunction(env, aggregateConfig), new ProcessWindowFunction(aggregateConfig));
                    break;
                case SLIDING_PROCESSING_TIME:
                    singleOutputStreamOperator = input
                            .keyBy(new KeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(SlidingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide())))
                            .aggregate(new AggregateProcessorFunction(env, aggregateConfig), new ProcessWindowFunction(aggregateConfig));
                    break;
                case SLIDING_EVENT_TIME:
                    singleOutputStreamOperator = input
                            .keyBy(new KeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(SlidingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide())))
                            .aggregate(new AggregateProcessorFunction(env, aggregateConfig), new ProcessWindowFunction(aggregateConfig));
                    break;
                default:
                    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Invalid window type");
            }
        }
        if (aggregateConfig.getParallelism() != 0) {
            singleOutputStreamOperator.setParallelism(aggregateConfig.getParallelism());
        }
        return singleOutputStreamOperator.name(aggregateConfig.getName());

    }


    @Override
    public AggregateConfig parseConfig(String name, Config config) {

        CheckResult result = CheckConfigUtil.checkAllExists(config,
                AggregateConfigOptions.GROUP_BY_FIELDS.key(),
                AggregateConfigOptions.WINDOW_TYPE.key(),
                AggregateConfigOptions.FUNCTIONS.key(),
                AggregateConfigOptions.WINDOW_SIZE.key());
        if (!result.isSuccess()) {
            throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                    "Aggregate processor: %s, At least one of [%s] should be specified.",
                    name, String.join(",",
                            AggregateConfigOptions.OUTPUT_FIELDS.key(),
                            AggregateConfigOptions.REMOVE_FIELDS.key(),
                            AggregateConfigOptions.FUNCTIONS.key())));
        }

        AggregateConfig aggregateConfig = new JSONObject(config.root().unwrapped()).toJavaObject(AggregateConfig.class);
        aggregateConfig.setName(name);
        return aggregateConfig;
    }

}
