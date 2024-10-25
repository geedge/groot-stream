package com.geedgenetworks.core.processor.aggregate;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.config.AggregateConfigOptions;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.AggregateConfig;
import com.typesafe.config.Config;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

import static com.geedgenetworks.common.Constants.*;

public class AggregateProcessorImpl implements AggregateProcessor {

    @Override
    public DataStream<Event> processorFunction(DataStream<Event> grootEventSingleOutputStreamOperator, AggregateConfig aggregateConfig, ExecutionConfig config) throws Exception {

        SingleOutputStreamOperator<Event> singleOutputStreamOperator;
        if (aggregateConfig.getMini_batch()) {
            switch (aggregateConfig.getWindow_type()) {
                case TUMBLING_PROCESSING_TIME:
                    singleOutputStreamOperator = grootEventSingleOutputStreamOperator
                            .process(new FirstAggregationProcessingTime(aggregateConfig, aggregateConfig.getWindow_size()))
                            .keyBy(new PreKeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(TumblingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size())))
                            .aggregate(new SecondAggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl(aggregateConfig));
                    break;
                case TUMBLING_EVENT_TIME:
                    singleOutputStreamOperator = grootEventSingleOutputStreamOperator
                            .process(new FirstAggregationEventTime(aggregateConfig, aggregateConfig.getWindow_size()))
                            .keyBy(new PreKeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(TumblingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size())))
                            .aggregate(new SecondAggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl(aggregateConfig));
                    break;
                case SLIDING_PROCESSING_TIME:
                    singleOutputStreamOperator = grootEventSingleOutputStreamOperator
                            .process(new FirstAggregationProcessingTime(aggregateConfig, aggregateConfig.getWindow_slide()))
                            .keyBy(new PreKeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(SlidingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide())))
                            .aggregate(new SecondAggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl(aggregateConfig));
                    break;
                case SLIDING_EVENT_TIME:
                    singleOutputStreamOperator = grootEventSingleOutputStreamOperator
                            .process(new FirstAggregationEventTime(aggregateConfig, aggregateConfig.getWindow_slide()))
                            .keyBy(new PreKeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(SlidingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide())))
                            .aggregate(new SecondAggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl(aggregateConfig));
                    break;
                default:
                    throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Invalid window type");

            }
        } else {
            switch (aggregateConfig.getWindow_type()) {
                case TUMBLING_PROCESSING_TIME:
                    singleOutputStreamOperator = grootEventSingleOutputStreamOperator
                            .keyBy(new KeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(TumblingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size())))
                            .aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl(aggregateConfig));
                    break;
                case TUMBLING_EVENT_TIME:
                    singleOutputStreamOperator = grootEventSingleOutputStreamOperator
                            .keyBy(new KeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(TumblingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size())))
                            .aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl(aggregateConfig));
                    break;
                case SLIDING_PROCESSING_TIME:
                    singleOutputStreamOperator = grootEventSingleOutputStreamOperator
                            .keyBy(new KeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(SlidingProcessingTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide())))
                            .aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl(aggregateConfig));
                    break;
                case SLIDING_EVENT_TIME:
                    singleOutputStreamOperator = grootEventSingleOutputStreamOperator
                            .keyBy(new KeySelector(aggregateConfig.getGroup_by_fields()))
                            .window(SlidingEventTimeWindows.of(Time.seconds(aggregateConfig.getWindow_size()), Time.seconds(aggregateConfig.getWindow_slide())))
                            .aggregate(new AggregateProcessorFunction(aggregateConfig, config), new ProcessWindowFunctionImpl(aggregateConfig));
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
    public String type() {
        return "aggregate";
    }

    @Override
    public AggregateConfig checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig) {
        CheckResult result = CheckConfigUtil.checkAllExists(typeSafeConfig.getConfig(name),
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

        AggregateConfig aggregateConfig = new JSONObject(configProperties).toJavaObject(AggregateConfig.class);
        aggregateConfig.setName(name);
        return aggregateConfig;
    }

}
