package com.geedgenetworks.connectors.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConnectorOptionsUtil {

    public static final String PROPERTIES_PREFIX = "kafka.";
    public static final String HEADERS_PREFIX = "headers.";

    public static Properties getKafkaProperties(Map<String, String> tableOptions) {
        final Properties kafkaProperties = new Properties();

        if (hasKafkaClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                final String subKey = key.substring((PROPERTIES_PREFIX).length());
                                kafkaProperties.put(subKey, value);
                            });
        }
        return kafkaProperties;
    }

    public static Map<String, String> getKafkaHeaders(Map<String, String> tableOptions) {
        final Map<String, String> headers = new HashMap<>();

        tableOptions.keySet().stream()
                .filter(key -> key.startsWith(HEADERS_PREFIX))
                .forEach(
                        key -> {
                            final String value = tableOptions.get(key);
                            final String subKey = key.substring((HEADERS_PREFIX).length());
                            headers.put(subKey, value);
                        });

        return headers;
    }

    /**
     * Decides if the table options contains Kafka client properties that start with prefix
     * 'properties'.
     */
    private static boolean hasKafkaClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }
}
