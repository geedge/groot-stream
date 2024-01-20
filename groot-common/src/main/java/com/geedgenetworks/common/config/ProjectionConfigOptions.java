package com.geedgenetworks.common.config;

import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.common.udf.UDFContext;

import java.util.List;

public interface ProjectionConfigOptions {
    Option<String> TYPE = Options.key("type")
            .stringType()
            .noDefaultValue()
            .withDescription("The type of processor.");

    Option<List<String>> OUTPUT_FIELDS = Options.key("output_fields")
            .listType()
            .noDefaultValue()
            .withDescription("The fields to be outputted.");

    Option<List<String>> REMOVE_FIELDS = Options.key("remove_fields")
            .listType()
            .noDefaultValue()
            .withDescription("The fields to be removed.");

    Option<List<UDFContext>> FUNCTIONS = Options.key("functions")
            .type(new TypeReference<List<UDFContext>>() {})
            .noDefaultValue()
            .withDescription("The functions to be executed.");






}
