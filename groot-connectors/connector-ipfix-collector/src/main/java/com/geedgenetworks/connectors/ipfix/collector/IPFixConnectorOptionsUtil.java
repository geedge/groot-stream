package com.geedgenetworks.connectors.ipfix.collector;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Properties;

public class IPFixConnectorOptionsUtil {

    public static final String PROPERTIES_PREFIX = "service.";

    public static Properties getServiceDiscoveryProperties(Map<String, String> tableOptions) {
        final Properties serviceDiscoveryProperties = new Properties();

        if (hasServiceDiscoveryProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(
                            key -> {
                                final String value = tableOptions.get(key);
                                if (!StringUtils.isBlank(value)) {
                                    serviceDiscoveryProperties.put(key, value);
                                }
                            });
        }
        return serviceDiscoveryProperties;
    }

    /**
     * Decides if the table options contains Kafka client properties that start with prefix
     * 'properties'.
     */
    private static boolean hasServiceDiscoveryProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }
}
