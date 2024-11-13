package com.geedgenetworks.api.connector.source;

import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.common.config.Option;
import com.geedgenetworks.common.config.Options;

import java.util.List;
import java.util.Map;

public interface SourceConfigOptions {
    Option<String> TYPE = Options.key("type")
            .stringType()
            .noDefaultValue()
            .withDescription("The type of source.");

    Option<Map<String, String>> PROPERTIES = Options.key("properties")
            .mapType()
            .noDefaultValue()
            .withDescription("Custom properties for source.");

    Option<List<Map<String, String>>> FIELDS = Options.key("fields")
            .type(new TypeReference<List<Map<String, String>>>() {})
            .noDefaultValue()
            .withDescription("When fields is not specified, all fields in source will be outputted as format of Map<String, Object>.");




}
