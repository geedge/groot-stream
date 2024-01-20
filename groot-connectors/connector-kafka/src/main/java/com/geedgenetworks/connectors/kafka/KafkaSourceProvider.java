package com.geedgenetworks.connectors.kafka;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.connector.format.DecodingFormat;
import com.geedgenetworks.core.connector.source.SourceProvider;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.List;
import java.util.Properties;

public class KafkaSourceProvider implements SourceProvider {
    private final StructType physicalDataType;
    private final DeserializationSchema<Event> valueDeserialization;

    private final List<String> topics;
    private final Properties properties;

    public KafkaSourceProvider(
            StructType physicalDataType,
            DecodingFormat valueDecodingFormat,
            List<String> topics,
            Properties properties
    ) {
        this.physicalDataType = physicalDataType;
        this.valueDeserialization = valueDecodingFormat.createRuntimeDecoder(physicalDataType);
        this.topics = topics;
        this.properties = properties;

    }

    @Override
    public SingleOutputStreamOperator<Event> produceDataStream(StreamExecutionEnvironment env) {
        FlinkKafkaConsumer<Event> kafkaConsumer = new FlinkKafkaConsumer<>(
                topics,
                new EventKafkaDeserializationSchema(valueDeserialization),
                properties
        );

        return env.addSource(kafkaConsumer);
    }

    @Override
    public StructType getPhysicalDataType() {
        return physicalDataType;
    }
}
