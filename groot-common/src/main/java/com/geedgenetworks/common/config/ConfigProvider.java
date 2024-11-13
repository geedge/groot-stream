package com.geedgenetworks.common.config;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.client.config.impl.YamlClientConfigLocator;
import com.hazelcast.config.Config;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.internal.config.YamlConfigLocator;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.*;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmptyAfterTrim;

/**
 * Locates and loads grootStream configurations from various locations.
  */
public class ConfigProvider {
    private ConfigProvider() {}

    public static GrootStreamConfig locateAndGetGrootStreamConfig() {
        return locateAndGetGrootStreamConfig(null);
    }

    public static GrootStreamConfig locateAndGetGrootStreamConfig(Properties properties) {
        CommonConfigLocator yamlConfigLocator = new CommonConfigLocator();
        GrootStreamConfig config;
        validateSuffixInSystemProperty(Constants.SYSPROP_GROOTSTREAM_CONFIG);
       if (yamlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading YAML config if provided in system property and start job specific properties
            config = new GrootStreamConfigBuilder(yamlConfigLocator)
                    .setProperties(properties)
                    .build();
        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 2. Try loading YAML config from the working directory or from the classpath
            config = new GrootStreamConfigBuilder(yamlConfigLocator)
                    .setProperties(properties)
                    .build();
        } else {
            // 3. Loading the default YAML configuration file
            yamlConfigLocator.locateDefault();
            config = new GrootStreamConfigBuilder(yamlConfigLocator)
                    .setProperties(properties)
                    .build();
        }
        return config;
    }

    public static GrootStreamConfig locateAndGetGrootStreamConfigFromString(String source) {
        return locateAndGetGrootStreamConfigFromString(source, null);
    }

    public static GrootStreamConfig locateAndGetGrootStreamConfigFromString(String source, Properties properties) {
        GrootStreamConfig config;
        if (isNullOrEmptyAfterTrim(source)) {
            throw new IllegalArgumentException("Config string cannot be null or empty");
        }
        config = new GrootStreamConfigBuilder(new ByteArrayInputStream(source.getBytes()))
                .setProperties(properties)
                .build();
        return config;
    }


    public static ClientConfig locateAndGetClientConfig() {
        validateSuffixInSystemProperty(SYSPROP_CLIENT_CONFIG);

        ClientConfig config;
        YamlClientConfigLocator yamlConfigLocator = new YamlClientConfigLocator();
        if (yamlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading config if provided in system property, and it is an YAML file
            config = new YamlClientConfigBuilder(yamlConfigLocator.getIn()).build();
        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 2. Try loading YAML config from the working directory or from the classpath
            config = new YamlClientConfigBuilder(yamlConfigLocator.getIn()).build();
        } else {
            // 3. Loading the default YAML configuration file
            yamlConfigLocator.locateDefault();
            config = new YamlClientConfigBuilder(yamlConfigLocator.getIn()).build();
        }
        return config;
    }

    /**
     * Locates and loads Hazelcast configurations from various locations.
     * @param properties
     * @return
     */
    public static Config locateAndGetMemberConfig(Properties properties) {
        validateSuffixInSystemProperty(SYSPROP_MEMBER_CONFIG);

        Config config;
        YamlConfigLocator yamlConfigLocator = new YamlConfigLocator();

        if (yamlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading config if provided in system property, and it is an YAML file
            config =
                    new YamlConfigBuilder(yamlConfigLocator.getIn())
                            .setProperties(properties)
                            .build();
        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 2. Try loading YAML config from the working directory or from the classpath
            config =
                    new YamlConfigBuilder(yamlConfigLocator.getIn())
                            .setProperties(properties)
                            .build();
        } else {
            // 3. Loading the default YAML configuration file
            yamlConfigLocator.locateDefault();
            config =
                    new YamlConfigBuilder(yamlConfigLocator.getIn())
                            .setProperties(properties)
                            .build();
        }
        return config;
    }

    public static List<String> locateAndGetUDFPluginConfig(Properties properties) {

        List<String> config;

        UDFPluginConfigLocator udfPluginConfigLocator = new UDFPluginConfigLocator();
        if (udfPluginConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading config if provided in system property
            config =
                    new UDFPluginConfigBuilder(udfPluginConfigLocator.getIn())
                            .setProperties(properties)
                            .build();
        } else if (udfPluginConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 2. Try loading config from the working directory or from the classpath
            config =
                    new UDFPluginConfigBuilder(udfPluginConfigLocator.getIn())
                            .setProperties(properties)
                            .build();
        } else {
            // 3. Loading the default configuration file
            udfPluginConfigLocator.locateDefault();
            config =
                    new UDFPluginConfigBuilder(udfPluginConfigLocator.getIn())
                            .setProperties(properties)
                            .build();
        }
        return config;
    }
}
