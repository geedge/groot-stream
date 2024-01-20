package com.geedgenetworks.common.config;

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
