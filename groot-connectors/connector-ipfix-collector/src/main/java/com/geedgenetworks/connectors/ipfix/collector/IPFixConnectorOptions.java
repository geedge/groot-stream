package com.geedgenetworks.connectors.ipfix.collector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class IPFixConnectorOptions {

    public static final ConfigOption<String> PORT_RANGE =
            ConfigOptions.key("port.range")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Initial port of the IPFix server.");

    public static final ConfigOption<Integer> MAX_PACKET_SIZE =
            ConfigOptions.key("max.packet.size")
                    .intType()
                    .defaultValue(65535);

    public static final ConfigOption<Integer> MAX_RECEIVE_BUFFER_SIZE =
            ConfigOptions.key("max.receive.buffer.size")
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
                    .defaultValue("127.0.0.1:8848");

    public static final ConfigOption<String> SERVICE_DISCOVERY_NACOS_USERNAME =
            ConfigOptions.key("service.discovery.nacos.username")
                    .stringType()
                    .defaultValue("nacos");

    public static final ConfigOption<String> SERVICE_DISCOVERY_NACOS_PASSWORD =
            ConfigOptions.key("service.discovery.nacos.password")
                    .stringType()
                    .defaultValue("nacos");

    public static final ConfigOption<String> SERVICE_DISCOVERY_NACOS_NAMESPACE =
            ConfigOptions.key("service.discovery.nacos.namespace")
                    .stringType()
                    .defaultValue("public");

    public static final ConfigOption<String> SERVICE_DISCOVERY_NACOS_GROUP =
            ConfigOptions.key("service.discovery.nacos.group")
                    .stringType()
                    .defaultValue("DEFAULT_GROUP");

    public static final ConfigOption<String> SERVICE_DISCOVERY_CONSUL_SERVER_IP =
            ConfigOptions.key("service.discovery.consul.server.ip")
                    .stringType()
                    .defaultValue("127.0.0.1");

    public static final ConfigOption<Integer> SERVICE_DISCOVERY_CONSUL_SERVER_PORT =
            ConfigOptions.key("service.discovery.consul.server.port")
                    .intType()
                    .defaultValue(8500);

    public static final ConfigOption<String> SERVICE_DISCOVERY_CONSUL_TOKEN =
            ConfigOptions.key("service.discovery.consul.token")
                    .stringType()
                    .defaultValue("");

}
