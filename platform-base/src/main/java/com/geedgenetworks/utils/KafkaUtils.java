package com.geedgenetworks.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

            if(entry.getKey().contains("kafka.") && !"".equals(entry.getValue()) && entry.getValue()!=null) {
                String key = entry.getKey().replace("kafka.","");
                properties.put(key, entry.getValue());
                System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
            }


        }
       /* properties.setProperty("bootstrap.servers", String.valueOf(kafkaconfig.get("bootstrap_servers")));

        properties.put("acks", "1");
        properties.put("retries", kafkaconfig.getOrDefault("retries",3));
        properties.put("linger.ms", kafkaconfig.getOrDefault("linger_ms",3));
        properties.put("request.timeout.ms", kafkaconfig.getOrDefault("request_timeout_ms",3));
        properties.put("batch.size", kafkaconfig.getOrDefault("batch_size",3));
        properties.put("buffer.memory", kafkaconfig.getOrDefault("buffer_memory",3));
        properties.put("max.request.size", kafkaconfig.getOrDefault("max_request_size",3));
        properties.put("compression.type", kafkaconfig.getOrDefault("compression_type","org.apache.kafka.common.serialization.StringDeserializer"));

        Integer security_type = (Integer) kafkaconfig.getOrDefault("security_type",0);

        switch (security_type) {
            case 1:
                properties.put("security.protocol", "SSL");
                properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
                properties.put("ssl.keystore.location", kafkaconfig.getOrDefault("ssl_keystore_location","") + "keystore.jks");
                properties.put("ssl.keystore.password", kafkaconfig.getOrDefault("ssl_keystore_password",""));
                properties.put("ssl.truststore.location", kafkaconfig.getOrDefault("ssl_truststore_location","") + "truststore.jks");
                properties.put("ssl.truststore.password", kafkaconfig.getOrDefault("ssl_truststore_password",""));
                properties.put("ssl.key.password", kafkaconfig.getOrDefault("ssl_key_password",""));
                break;
            case 2:
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "PLAIN");
                properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username="
                        + kafkaconfig.getOrDefault("sasl_user","") + " password=" + kafkaconfig.getOrDefault("sasl_password","")+ ";");
                break;
            default:
        }*/
        return properties;
    }


    public static Properties getKafkaSourceProperty(Map<String, Object> kafkaconfig) {
        Properties properties = new Properties();


        for (Map.Entry<String, Object> entry : kafkaconfig.entrySet()) {

            if(entry.getKey().contains("kafka.") && !"".equals(entry.getValue()) && entry.getValue()!=null) {
                String key = entry.getKey().replace("kafka.","");
                properties.put(key, entry.getValue());
                System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
            }


        }


       /* properties.setProperty("group.id",String.valueOf(kafkaconfig.get("bootstrap_servers")));
        properties.setProperty("bootstrap.servers", String.valueOf(kafkaconfig.get("bootstrap_servers")));
        properties.setProperty("session.timeout.ms", String.valueOf(kafkaconfig.get("session_timeout_ms")));
        properties.setProperty("max.poll.records", String.valueOf(kafkaconfig.get("max_poll_records")));
        properties.setProperty("max.partition.fetch.bytes", String.valueOf(kafkaconfig.get("max_partition_fetch_bytes")));

        Integer security_type = (Integer) kafkaconfig.getOrDefault("security_type",0);

        if(kafkaconfig.containsKey("kafka.security.protocol")) {
            switch (kafkaconfig.get("kafka.security.protocol").toString()) {
                case "SSL":
                    properties.put("security.protocol", "SSL");
                    properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
                    properties.put("ssl.keystore.location", kafkaconfig.getOrDefault("kafka.ssl.keystore.location", "") + "keystore.jks");
                    properties.put("ssl.keystore.password", kafkaconfig.getOrDefault("kafka.ssl.keystore.password", ""));
                    properties.put("ssl.truststore.location", kafkaconfig.getOrDefault("kafka.ssl.truststore.location", "") + "truststore.jks");
                    properties.put("ssl.truststore.password", kafkaconfig.getOrDefault("kafka.ssl.truststore.password", ""));
                    properties.put("ssl.key.password", kafkaconfig.getOrDefault("kafka.ssl.key.password", ""));
                    break;
                case "SASL_PLAINTEXT":
                    properties.put("security.protocol", "SASL_PLAINTEXT");
                    properties.put("sasl.mechanism", kafkaconfig.getOrDefault("kafka.sasl.mechanism", "PLAIN"));
                    properties.put("sasl.jaas.config", kafkaconfig.getOrDefault("kafka.sasl.jaas.config", ""));
                    break;
                default:
            }
        }*/
        return properties;
    }






    public static FlinkKafkaConsumer<String> getKafkaConsumer(Map<String, Object> kafkaconfig) {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>((String) kafkaconfig.get("topics"),
                new SimpleStringSchema(), getKafkaSourceProperty(kafkaconfig));

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }
    public static FlinkKafkaConsumer<String> getKafkaConsumerLists(List<String> topic,Map<String, Object> kafkaconfig) {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic,
                new SimpleStringSchema(), getKafkaSourceProperty(kafkaconfig));

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }
    public static SinkFunction<String> getKafkaSink(Map<String, Object> kafkaconfig) {
        return new FlinkKafkaProducer<String>(
                (String) kafkaconfig.get("kafka.topic"),
                new SimpleStringSchema(),
                getKafkaSinkProperty(kafkaconfig),
                Optional.empty()
        );
    }

}
