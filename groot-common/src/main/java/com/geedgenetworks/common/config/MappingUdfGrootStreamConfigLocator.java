package com.geedgenetworks.common.config;

import com.geedgenetworks.common.Constants;
import com.hazelcast.internal.config.AbstractConfigLocator;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

@Slf4j
public final class MappingUdfGrootStreamConfigLocator extends AbstractConfigLocator {

    public static Collection<String> ALL_ACCEPTED_SUFFIXES =Collections.unmodifiableList(Arrays.asList("plugin"));


    public MappingUdfGrootStreamConfigLocator() {
    }
    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty(Constants.MAPPING_GROOTSTREAM_UDF, ALL_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateFromSystemPropertyOrFailOnUnacceptedSuffix() {
        return loadFromSystemPropertyOrFailOnUnacceptedSuffix(
                Constants.MAPPING_GROOTSTREAM_UDF, ALL_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory(
                Constants.MAPPING_GROOTSTREAM_UDF, ALL_ACCEPTED_SUFFIXES);
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath(
                Constants.MAPPING_GROOTSTREAM_UDF, ALL_ACCEPTED_SUFFIXES);
    }

    @Override
    public boolean locateDefault() {
        loadDefaultConfigurationFromClasspath(Constants.MAPPING_GROOTSTREAM_UDF_DEFAULT_PLUGIN);
        return true;
    }
}
