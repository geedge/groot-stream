package com.geedgenetworks.common.config;

import java.util.List;
import java.util.Map;
import com.alibaba.fastjson2.TypeReference;
public interface UDFContextConfigOptions {
    Option<String> NAME = Options.key("name")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the function.");

    Option<List<String>> LOOKUP_FIELDS = Options.key("lookup_fields")
            .listType()
            .noDefaultValue()
            .withDescription("The fields to be looked up.");

    Option<List<String>> OUTPUT_FIELDS = Options.key("output_fields")
            .listType()
            .noDefaultValue()
            .withDescription("The fields to be outputted.");

    Option<String> FILTER = Options.key("filter")
            .stringType()
            .noDefaultValue()
            .withDescription("The filter expression to be applied.");

    Option<Map<String, Object>> PARAMETERS = Options.key("parameters")
            .type(new TypeReference<Map<String, Object>>() {})
            .noDefaultValue()
            .withDescription("The parameters for the function.");

    Option<String> FUNCTION = Options.key("function")
            .stringType()
            .noDefaultValue()
            .withDescription("The function to be executed.");
}
