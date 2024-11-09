package com.geedgenetworks.core.processor.aggregate;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.config.KeybyEntity;
import com.geedgenetworks.spi.processor.AggregateConfig;
import com.geedgenetworks.spi.table.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

@Slf4j
public class FirstAggregationEventTime extends AbstractFirstAggregation {

    private final PriorityQueue<Long> eventTimeTimersQueue = new PriorityQueue<>();

    public FirstAggregationEventTime(AggregateConfig aggregateConfig, long windowSize) {
        super(aggregateConfig, windowSize);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void processElement(Event value, ProcessFunction<Event, Accumulator>.Context ctx, Collector<Accumulator> out) throws Exception {
        Long timestamp;
        internalMetrics.incrementInEvents();
        try {
            KeybyEntity keybyEntity = getKey(value, groupByFields);
            while ((timestamp = eventTimeTimersQueue.peek()) != null && timestamp <= ctx.timerService().currentWatermark()) {
                eventTimeTimersQueue.poll();
                onTimer(timestamp, out);
            }
            long windowEnd = assignWindowEnd(ctx.timerService().currentWatermark());
            if (!windows.containsKey(windowEnd)) {
                Map<String, Accumulator> map = new HashMap<>();
                map.put(keybyEntity.getKeysToString(), createAccumulator(keybyEntity.getKeys()));
                windows.put(windowEnd, map);
                eventTimeTimersQueue.add(windowEnd);
            } else {
                if (!windows.get(windowEnd).containsKey(keybyEntity.getKeysToString())) {
                    windows.get(windowEnd).put(keybyEntity.getKeysToString(), createAccumulator(keybyEntity.getKeys()));
                }
            }
            add(value, windows.get(windowEnd).get(keybyEntity.getKeysToString()));
        } catch (Exception e) {
            log.error("Error in pre-aggregate processElement", e);
            internalMetrics.incrementErrorEvents();
        }
    }


}
