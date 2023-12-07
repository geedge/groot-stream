package com.geedgenetworks.connectors.ipfix.collector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class IPFixConnectorOptions {

    public static final ConfigOption<Integer> INITIAL_PORT =
            ConfigOptions.key("initial.port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Initial port of the IPFix server.");

    public static final ConfigOption<Integer> PORT_NUMBER =
            ConfigOptions.key("port.number")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Number of ports of the IPFix server.");

    public static final ConfigOption<Integer> MAX_ATTEMPTS =
            ConfigOptions.key("max.attempts")
                    .intType()
                    .defaultValue(1);

    public static final ConfigOption<Integer> BUFFER_SIZE =
            ConfigOptions.key("buffer.size")
                    .intType()
                    .defaultValue(65535);

    public static final ConfigOption<Integer> RECEIVE_BUFFER =
            ConfigOptions.key("receive.buffer")
                    .intType()
                    .defaultValue(104857600);

    public static final ConfigOption<Integer> SERVICE_DISCOVERY_REGISTRY_MODE =
            ConfigOptions.key("service.discovery.registry.mode")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Component used by service discovery.");

    public static final ConfigOption<String> SERVICE_DISCOVERY_SERVICE_NAME =
            ConfigOptions.key("service.discovery.service.name")
                    .stringType()
                    .defaultValue("udp_ipfix")
                    .withDescription("The service name used for service discovery.");

    public static final ConfigOption<Integer> SERVICE_DISCOVERY_HEALTH_CHECK_INTERVAL =
            ConfigOptions.key("service.discovery.health.check.interval")
                    .intType()
                    .defaultValue(5)
                    .withDescription("Required Kafka server connection string");

    public static final ConfigOption<String> SERVICE_DISCOVERY_NACOS_SERVER_ADDR =
            ConfigOptions.key("service.discovery.nacos.server.addr")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> SERVICE_DISCOVERY_NACOS_USERNAME =
            ConfigOptions.key("service.discovery.nacos.username")
                    .stringType()
                    .defaultValue("");

    public static final ConfigOption<String> SERVICE_DISCOVERY_NACOS_PASSWORD =
            ConfigOptions.key("service.discovery.nacos.password")
                    .stringType()
                    .defaultValue("");

    public static final ConfigOption<String> SERVICE_DISCOVERY_NACOS_NAMESPACE =
            ConfigOptions.key("service.discovery.nacos.namespace")
                    .stringType()
                    .defaultValue("public");

    public static final ConfigOption<String> SERVICE_DISCOVERY_NACOS_GROUP =
            ConfigOptions.key("service.discovery.nacos.group")
                    .stringType()
                    .defaultValue("DEFAULT_GROUP");

    public static final ConfigOption<String> SERVICE_DISCOVERY_CONSUL_SERVER_ADDR =
            ConfigOptions.key("service.discovery.consul.server.addr")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<Integer> SERVICE_DISCOVERY_CONSUL_SERVER_PORT =
            ConfigOptions.key("service.discovery.consul.server.port")
                    .intType()
                    .noDefaultValue();

    public static final ConfigOption<String> SERVICE_DISCOVERY_CONSUL_TOKEN =
            ConfigOptions.key("service.discovery.consul.token")
                    .stringType()
                    .defaultValue("");

}
