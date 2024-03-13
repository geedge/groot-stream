package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.RuntimeContextAware;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.SerializedValue;

import java.util.Map;
import java.util.Properties;

public class GrootKafkaFetcher<T> extends KafkaFetcher<T> {
    public GrootKafkaFetcher(
            SourceFunction.SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            ProcessingTimeService processingTimeProvider,
            long autoWatermarkInterval,
            ClassLoader userCodeClassLoader,
            String taskNameWithSubtasks,
            KafkaDeserializationSchema<T> deserializer,
            Properties kafkaProperties,
            long pollTimeout,
            MetricGroup subtaskMetricGroup,
            MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {
        super(sourceContext, assignedPartitionsWithInitialOffsets, watermarkStrategy, processingTimeProvider, autoWatermarkInterval, userCodeClassLoader, taskNameWithSubtasks, deserializer, kafkaProperties, pollTimeout, subtaskMetricGroup, consumerMetricGroup, useMetrics);
        if(deserializer instanceof RuntimeContextAware){
            ((RuntimeContextAware) deserializer).runtimeContextAware(runtimeContext);
        }
    }
}
