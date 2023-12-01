package com.geedgenetworks.connectors.kafka;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

import static com.geedgenetworks.connectors.kafka.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;

public class KafkaConnectorOptions {
    public static final ConfigOption<List<String>> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Topic names from which the table is read.");

    public static final ConfigOption<String> PROPS_BOOTSTRAP_SERVERS =
            ConfigOptions.key( PROPERTIES_PREFIX + "bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required Kafka server connection string");



}
