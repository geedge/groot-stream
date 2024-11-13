package com.geedgenetworks.api.factory;

import org.apache.flink.configuration.ConfigOption;

import java.util.Set;

public interface Factory {
    /**
     * Returns the factory identifier.
     * If multiple factories exist for different versions, a version should be appended using "-".
     * (e.g. {@code kafka-1}).
     */
    String type();

    Set<ConfigOption<?>> requiredOptions();
    Set<ConfigOption<?>> optionalOptions();
}
