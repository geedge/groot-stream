package com.geedgenetworks.connectors.kafka;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.connector.format.EncodingFormat;
import com.geedgenetworks.core.connector.sink.SinkProvider;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.kafka.GrootFlinkKafkaProducer;

import java.util.Optional;
import java.util.Properties;

public class KafkaSinkProvider implements SinkProvider {
    private final StructType dataType;
    private final SerializationSchema<Event> valueSerialization;

    private final String topic;
    private final Properties properties;
    private final boolean logFailuresOnly;

    public KafkaSinkProvider(
            StructType dataType,
            EncodingFormat valueEncodingFormat,
            String topic,
            Properties properties,
            boolean logFailuresOnly
    ) {
        this.dataType = dataType;
        this.valueSerialization = valueEncodingFormat.createRuntimeEncoder(dataType);
        this.topic = topic;
        this.properties = properties;
        this.logFailuresOnly = logFailuresOnly;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Event> dataStream) {
        GrootFlinkKafkaProducer<Event> kafkaProducer = new GrootFlinkKafkaProducer<>(
                topic,
                valueSerialization,
                properties,
                Optional.empty()
        );
        kafkaProducer.setLogFailuresOnly(logFailuresOnly);
        return dataStream.addSink(kafkaProducer);
    }
}
