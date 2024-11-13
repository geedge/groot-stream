package com.geedgenetworks.api.connector.sink;

import com.geedgenetworks.common.config.Option;
import com.geedgenetworks.common.config.Options;

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
