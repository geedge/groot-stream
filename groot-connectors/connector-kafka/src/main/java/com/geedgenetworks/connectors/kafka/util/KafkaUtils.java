package com.geedgenetworks.connectors.kafka.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class KafkaUtils {

    public static Properties getKafkaSinkProperty(Map<String, Object> kafkaconfig) {
        Properties properties = new Properties();
        for (Map.Entry<String, Object> entry : kafkaconfig.entrySet()) {

            if (entry.getKey().contains("kafka.")
                    && !"".equals(entry.getValue())
                    && entry.getValue() != null) {
                String key = entry.getKey().replace("kafka.", "");
                properties.put(key, entry.getValue());
            }
        }

        return properties;
    }

    public static Properties getKafkaSourceProperty(Map<String, Object> kafkaconfig) {
        Properties properties = new Properties();

        for (Map.Entry<String, Object> entry : kafkaconfig.entrySet()) {

            if (entry.getKey().contains("kafka.")
                    && !"".equals(entry.getValue())
                    && entry.getValue() != null) {
                String key = entry.getKey().replace("kafka.", "");
                properties.put(key, entry.getValue());
            }
        }
        return properties;
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(Map<String, Object> kafkaconfig) {
        FlinkKafkaConsumer<String> kafkaConsumer =
                new FlinkKafkaConsumer<>(
                        (String) kafkaconfig.get("topics"),
                        new SimpleStringSchema(),
                        getKafkaSourceProperty(kafkaconfig));

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }

    public static FlinkKafkaConsumer<Tuple2<String, Long>> timestampDeserializationConsumer(
            Map<String, Object> kafkaconfig) {
        FlinkKafkaConsumer<Tuple2<String, Long>> kafkaConsumer =
                new FlinkKafkaConsumer<>(
                        (List<String>) kafkaconfig.get("topics"),
                        new TimestampDeserializationSchema(),
                        getKafkaSourceProperty(kafkaconfig));

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumerLists(
            List<String> topics, Map<String, Object> kafkaconfig) {
        FlinkKafkaConsumer<String> kafkaConsumer =
                new FlinkKafkaConsumer<>(
                        topics, new SimpleStringSchema(), getKafkaSourceProperty(kafkaconfig));

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }

    public static SinkFunction<String> getKafkaSink(Map<String, Object> kafkaconfig) {
        return new FlinkKafkaProducer<String>(
                (String) kafkaconfig.get("topic"),
                new SimpleStringSchema(),
                getKafkaSinkProperty(kafkaconfig),
                Optional.empty());
    }
}
