package com.geedgenetworks.core.processor.table;

import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.common.config.Option;
import com.geedgenetworks.common.config.Options;

import java.util.List;

public interface TableConfigOptions {

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
