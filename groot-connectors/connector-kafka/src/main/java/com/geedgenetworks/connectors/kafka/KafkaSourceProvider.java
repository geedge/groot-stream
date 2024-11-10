package com.geedgenetworks.connectors.kafka;

import com.geedgenetworks.spi.table.connector.DecodingFormat;
import com.geedgenetworks.spi.source.SourceProvider;
import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.GrootFlinkKafkaConsumer;

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
        GrootFlinkKafkaConsumer<Event> kafkaConsumer = new GrootFlinkKafkaConsumer<>(
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
