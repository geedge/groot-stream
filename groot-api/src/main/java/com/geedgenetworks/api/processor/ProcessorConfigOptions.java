package com.geedgenetworks.api.processor;

import com.geedgenetworks.common.config.Option;
import com.geedgenetworks.common.config.Options;
import java.util.Map;

public interface ProcessorConfigOptions {
    Option<String> NAME = Options.key("name")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the operator.");

    Option<String> TYPE = Options.key("type")
            .stringType()
            .noDefaultValue()
            .withDescription("The type of operator.");

    Option<Integer> PARALLELISM = Options.key("parallelism")
            .intType()
            .defaultValue(1)
            .withDescription("The parallelism of the operator.");

    Option<Map<String, String>> PROPERTIES = Options.key("properties")
            .mapType()
            .noDefaultValue()
            .withDescription("Custom properties for sink.");







}
