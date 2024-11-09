package com.geedgenetworks.spi.configuration;

import com.geedgenetworks.common.config.Option;
import com.geedgenetworks.common.config.Options;

import java.util.Map;

public interface FilterConfigOptions {
    Option<String> TYPE = Options.key("type")
            .stringType()
            .noDefaultValue()
            .withDescription("The type of filter .");

    Option<Map<String, String>> PROPERTIES = Options.key("properties")
            .mapType()
            .noDefaultValue()
            .withDescription("Custom properties for filter.");
}
