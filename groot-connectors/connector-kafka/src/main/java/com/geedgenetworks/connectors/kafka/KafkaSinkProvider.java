package com.geedgenetworks.connectors.kafka;

import com.geedgenetworks.connectors.kafka.rate.RateLimitingStrategy;
import com.geedgenetworks.api.connector.serialization.EncodingFormat;
import com.geedgenetworks.api.connector.sink.SinkProvider;
import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.kafka.GrootFlinkKafkaProducer;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class KafkaSinkProvider implements SinkProvider {
    private final StructType dataType;
    private final SerializationSchema<Event> valueSerialization;

    private final String topic;
    private final Properties properties;
    private final Map<String, String> headers;
    private final boolean logFailuresOnly;
    private final RateLimitingStrategy rateLimitingStrategy;

    public KafkaSinkProvider(
            StructType dataType,
            EncodingFormat valueEncodingFormat,
            String topic,
            Properties properties,
            Map<String, String> headers,
            boolean logFailuresOnly,
            RateLimitingStrategy rateLimitingStrategy
    ) {
        this.dataType = dataType;
        this.valueSerialization = valueEncodingFormat.createRuntimeEncoder(dataType);
        this.topic = topic;
        this.properties = properties;
        this.headers = headers;
        this.logFailuresOnly = logFailuresOnly;
        this.rateLimitingStrategy = rateLimitingStrategy;
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
        kafkaProducer.setRateLimitingStrategy(rateLimitingStrategy);
        kafkaProducer.setHeaders(headers);
        return dataStream.addSink(kafkaProducer);
    }
}
