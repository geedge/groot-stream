package com.geedgenetworks.api.factory;

import org.apache.flink.configuration.ConfigOption;

import java.util.Set;

public interface FormatFactory extends Factory {
    Set<ConfigOption<?>> requiredOptions();
    Set<ConfigOption<?>> optionalOptions();
}
