package com.geedgenetworks.common.config;

import com.hazelcast.config.AbstractYamlConfigBuilder;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.yaml.YamlDomChecker;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.internal.yaml.YamlNode;
import lombok.NonNull;
import org.w3c.dom.Node;

import java.io.InputStream;
import java.util.Properties;

import static com.geedgenetworks.common.config.ConfigProvider.locateAndGetMappingConfig;
import static com.hazelcast.internal.config.yaml.W3cDomUtil.asW3cNode;

public class YamlGrootStreamConfigBuilder extends AbstractYamlConfigBuilder {
    private final InputStream in;

    public YamlGrootStreamConfigBuilder() {
        this((YamlGrootStreamConfigLocator) null);
    }

    public YamlGrootStreamConfigBuilder(YamlGrootStreamConfigLocator locator) {

        if (locator == null) {
            locator = new YamlGrootStreamConfigLocator();
            locator.locateEverywhere();
        }
        this.in = locator.getIn();
    }

    public YamlGrootStreamConfigBuilder(@NonNull InputStream inputStream) {
        this.in = inputStream;
    }

    @Override
    protected String getConfigRoot() {
        return GrootStreamConfigSections.GROOTSTREAM.name;
    }

    public GrootStreamConfig build() {
        return build(new GrootStreamConfig());
    }

    public GrootStreamConfig build(GrootStreamConfig config) {
        try {
            parseAndBuildConfig(config);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        config.setHazelcastConfig(ConfigProvider.locateAndGetMemberConfig(getProperties()));
        config.setUdfMappingConfig(ConfigProvider.locateAndGetMappingConfig(getProperties()));
        return config;
    }

    private void parseAndBuildConfig(GrootStreamConfig config) throws Exception {
        YamlMapping yamlRootNode;
        try {
            yamlRootNode = (YamlMapping) YamlLoader.load(in);
        } catch (Exception ex) {
            throw new InvalidConfigurationException("Invalid YAML configuration", ex);
        } finally {
            IOUtil.closeResource(in);
        }
        YamlNode grootStreamRoot =
                yamlRootNode.childAsMapping(GrootStreamConfigSections.GROOTSTREAM.name);
        if (grootStreamRoot == null) {
            grootStreamRoot = yamlRootNode;
        }

        YamlDomChecker.check(grootStreamRoot);

        Node w3cRootNode = asW3cNode(grootStreamRoot);
        replaceVariables(w3cRootNode);
        importDocuments(grootStreamRoot);

        new YamlGrootStreamDomConfigProcessor(true, config).buildConfig(w3cRootNode);
    }
    public YamlGrootStreamConfigBuilder setProperties(Properties properties) {
        if (properties == null) {
            properties = System.getProperties();
        }
        setPropertiesInternal(properties);
        return this;
    }
}
