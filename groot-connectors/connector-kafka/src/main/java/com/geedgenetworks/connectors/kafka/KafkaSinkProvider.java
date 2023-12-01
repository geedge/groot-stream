package com.geedgenetworks.connectors.kafka;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.connector.format.EncodingFormat;
import com.geedgenetworks.core.connector.sink.SinkProvider;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Optional;
import java.util.Properties;

public class KafkaSinkProvider implements SinkProvider {
    private final StructType dataType;
    private final SerializationSchema<Event> valueSerialization;

    private final String topic;
    private final Properties properties;

    public KafkaSinkProvider(
            StructType dataType,
            EncodingFormat valueEncodingFormat,
            String topic,
            Properties properties
    ) {
        this.dataType = dataType;
        this.valueSerialization = valueEncodingFormat.createRuntimeEncoder(dataType);
        this.topic = topic;
        this.properties = properties;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Event> dataStream) {
        FlinkKafkaProducer<Event> kafkaProducer = new FlinkKafkaProducer<>(
                topic,
                valueSerialization,
                properties,
                Optional.empty()
        );
        return dataStream.addSink(kafkaProducer);
    }
}
