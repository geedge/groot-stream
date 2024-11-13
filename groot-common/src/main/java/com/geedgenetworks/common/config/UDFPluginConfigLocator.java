package com.geedgenetworks.common.config;

import com.hazelcast.internal.config.AbstractConfigLocator;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@Slf4j
public final class UDFPluginConfigLocator extends AbstractConfigLocator {

    public static Collection<String> ALL_ACCEPTED_SUFFIXES = Collections.unmodifiableList(Arrays.asList("plugins"));


    public UDFPluginConfigLocator() {
    }
    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty(Constants.SYSPROP_UDF_PLUGIN_CONFIG, ALL_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateFromSystemPropertyOrFailOnUnacceptedSuffix() {
        return loadFromSystemPropertyOrFailOnUnacceptedSuffix(
                Constants.SYSPROP_UDF_PLUGIN_CONFIG, ALL_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory(
                Constants.HAZELCAST_UDF_PLUGIN_CONFIG_FILE_PREFIX, ALL_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath(
                Constants.HAZELCAST_UDF_PLUGIN_CONFIG_FILE_PREFIX, ALL_ACCEPTED_SUFFIXES);
    }

    @Override
    public boolean locateDefault() {
        loadDefaultConfigurationFromClasspath(Constants.HAZELCAST_UDF_PLUGIN_CONFIG_DEFAULT);
        return true;
    }
}
