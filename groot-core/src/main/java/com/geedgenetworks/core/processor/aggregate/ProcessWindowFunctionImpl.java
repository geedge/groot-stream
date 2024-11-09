package com.geedgenetworks.core.processor.aggregate;

import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.config.KeybyEntity;
import com.geedgenetworks.common.utils.ColumnUtil;
import com.geedgenetworks.core.metrics.InternalMetrics;
import com.geedgenetworks.spi.processor.AggregateConfig;
import com.geedgenetworks.spi.table.event.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static com.geedgenetworks.spi.table.event.Event.WINDOW_END_TIMESTAMP;
import static com.geedgenetworks.spi.table.event.Event.WINDOW_START_TIMESTAMP;

public class ProcessWindowFunctionImpl extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
        Accumulator,                  // 输入类型
        Event,  // 输出类型
        KeybyEntity,                         // 键类型
        TimeWindow> {
    private final AggregateConfig aggregateConfig;
    private transient InternalMetrics internalMetrics;

    public ProcessWindowFunctionImpl(AggregateConfig aggregateConfig) {
        this.aggregateConfig = aggregateConfig;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.internalMetrics = new InternalMetrics(getRuntimeContext());
    }

    @Override
    public void process(KeybyEntity keybyEntity, ProcessWindowFunction<Accumulator, Event, KeybyEntity, TimeWindow>.Context context, Iterable<Accumulator> elements, Collector<Event> out) throws Exception {
        Accumulator accumulator = elements.iterator().next();
        Event event = new Event();
        event.setExtractedFields(accumulator.getMetricsFields());
        event.getExtractedFields().put(WINDOW_START_TIMESTAMP, context.window().getStart());
        event.getExtractedFields().put(WINDOW_END_TIMESTAMP, context.window().getEnd());
        event.getExtractedFields().putAll(keybyEntity.getKeys());
        internalMetrics.incrementOutEvents();
        internalMetrics.incrementErrorEvents(accumulator.getErrorCount());
        internalMetrics.incrementInEvents(accumulator.getInEvents());
        if (aggregateConfig.getWindow_timestamp_field() != null) {
            event.getExtractedFields().put(aggregateConfig.getWindow_timestamp_field(), context.window().getStart());
        }
        if (aggregateConfig.getOutput_fields() != null
                && !aggregateConfig.getOutput_fields().isEmpty()) {
            event.setExtractedFields(
                    ColumnUtil.columnSelector(
                            event.getExtractedFields(), aggregateConfig.getOutput_fields()));
        }
        if (aggregateConfig.getRemove_fields() != null
                && !aggregateConfig.getRemove_fields().isEmpty()) {
            event.setExtractedFields(
                    ColumnUtil.columnRemover(
                            event.getExtractedFields(), aggregateConfig.getRemove_fields()));
        }

        out.collect(event);
    }
}

