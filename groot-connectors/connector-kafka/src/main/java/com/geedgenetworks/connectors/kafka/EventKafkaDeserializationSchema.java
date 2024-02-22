package com.geedgenetworks.connectors.kafka;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.metrics.InternalMetrics;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.RuntimeContextAware;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventKafkaDeserializationSchema implements KafkaDeserializationSchema<Event>, RuntimeContextAware {
    private static final Logger LOG = LoggerFactory.getLogger(EventKafkaDeserializationSchema.class);
    private final DeserializationSchema<Event> valueDeserialization;
    private transient InternalMetrics internalMetrics;

    public EventKafkaDeserializationSchema(DeserializationSchema<Event> valueDeserialization) {
        this.valueDeserialization = valueDeserialization;
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        valueDeserialization.open(context);
    }

    // 解析失败，不发下游
    @Override
    public Event deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        throw new IllegalStateException("A collector is required for deserializing.");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> out) throws Exception {
        internalMetrics.incrementInEvents();
        internalMetrics.incrementInBytes(record.value().length);

        try {
            Event event = valueDeserialization.deserialize(record.value());
            if(event != null){
                event.getExtractedFields().put(Event.INTERNAL_TIMESTAMP_KEY, record.timestamp());
                out.collect(event);
                internalMetrics.incrementOutEvents();
                return;
            }
        }catch (Exception e) {
            LOG.error("反序列化失败", e);
        }
        internalMetrics.incrementErrorEvents();
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }

    @Override
    public void runtimeContextAware(RuntimeContext runtimeContext) {
        internalMetrics = new InternalMetrics(runtimeContext);
    }
}
