package com.geedgenetworks.common.config;

import java.util.Map;

public interface SinkConfigOptions {
    Option<String> TYPE = Options.key("type")
            .stringType()
            .noDefaultValue()
            .withDescription("The type of sink.");

    Option<Map<String, String>> PROPERTIES = Options.key("properties")
            .mapType()
            .noDefaultValue()
            .withDescription("Custom properties for sink.");
}
