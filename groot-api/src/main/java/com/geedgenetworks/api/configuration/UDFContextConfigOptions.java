package com.geedgenetworks.api.configuration;

import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.common.config.Option;
import com.geedgenetworks.common.config.Options;

import java.util.List;
import java.util.Map;

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

    Option<String> PARAMETERS_KB_NAME = Options.key("kb_name")
            .stringType()
            .noDefaultValue()
            .withDescription("The name of the knowledge base.");

    Option<String> PARAMETERS_OPTION = Options.key("option")
            .stringType()
            .noDefaultValue()
            .withDescription("The option for the function.");

    Option<Map<String, String>> PARAMETERS_GEOLOCATION_FIELD_MAPPING = Options.key("geolocation_field_mapping")
            .mapType()
            .noDefaultValue()
            .withDescription("The geolocation field mapping.");

    Option<String> PARAMETERS_IDENTIFIER = Options.key("identifier")
            .stringType()
            .noDefaultValue()
            .withDescription("The identifier for the parameters of function.");

    Option<String> PARAMETERS_SECRET_KEY = Options.key("secret_key")
            .stringType()
            .noDefaultValue()
            .withDescription("The secret key for the function.");

    Option<String> FUNCTION = Options.key("function")
            .stringType()
            .noDefaultValue()
            .withDescription("The function to be executed.");


}
