package com.geedgenetworks.common.config;

import com.hazelcast.internal.config.AbstractConfigLocator;

import static com.hazelcast.internal.config.DeclarativeConfigUtil.YAML_ACCEPTED_SUFFIXES;

public final class CommonConfigLocator extends AbstractConfigLocator {

    public CommonConfigLocator() {
    }
    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty(Constants.SYSPROP_GROOTSTREAM_CONFIG, YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateFromSystemPropertyOrFailOnUnacceptedSuffix() {
        return loadFromSystemPropertyOrFailOnUnacceptedSuffix(
                Constants.SYSPROP_GROOTSTREAM_CONFIG, YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory(
                Constants.HAZELCAST_GROOTSTREAM_CONFIG_FILE_PREFIX, YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath(
                Constants.HAZELCAST_GROOTSTREAM_CONFIG_FILE_PREFIX, YAML_ACCEPTED_SUFFIXES);
    }

    @Override
    public boolean locateDefault() {
        loadDefaultConfigurationFromClasspath(Constants.HAZELCAST_GROOTSTREAM_CONFIG_DEFAULT);
        return true;
    }
}
