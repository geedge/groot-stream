package com.geedgenetworks.common.config;

import com.geedgenetworks.common.Constants;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.client.config.impl.YamlClientConfigLocator;
import com.hazelcast.config.Config;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.internal.config.YamlConfigLocator;

import java.io.ByteArrayInputStream;
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
        YamlGrootStreamConfigLocator yamlConfigLocator = new YamlGrootStreamConfigLocator();
        GrootStreamConfig config;
        validateSuffixInSystemProperty(Constants.SYSPROP_GROOTSTREAM_CONFIG);
       if (yamlConfigLocator.locateFromSystemProperty()) {
            // 1. Try loading YAML config if provided in system property and start job specific properties
            config = new YamlGrootStreamConfigBuilder(yamlConfigLocator)
                    .setProperties(properties)
                    .build();
        } else if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 2. Try loading YAML config from the working directory or from the classpath
            config = new YamlGrootStreamConfigBuilder(yamlConfigLocator)
                    .setProperties(properties)
                    .build();
        } else {
            // 3. Loading the default YAML configuration file
            yamlConfigLocator.locateDefault();
            config = new YamlGrootStreamConfigBuilder(yamlConfigLocator)
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
        config = new YamlGrootStreamConfigBuilder(new ByteArrayInputStream(source.getBytes()))
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

}
