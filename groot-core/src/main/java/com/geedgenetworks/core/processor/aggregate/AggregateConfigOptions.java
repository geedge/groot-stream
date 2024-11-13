package com.geedgenetworks.core.processor.aggregate;

import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.common.config.Option;
import com.geedgenetworks.common.config.Options;

import java.util.List;

public interface AggregateConfigOptions {
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

    Option<List<String>> GROUP_BY_FIELDS = Options.key("group_by_fields")
            .listType()
            .noDefaultValue()
            .withDescription("The fields to be key by.");

    Option<String> WINDOW_TYPE = Options.key("window_type")
            .stringType()
            .noDefaultValue()
            .withDescription("The type of window.");

    Option<Integer> WINDOW_SIZE = Options.key("window_size")
            .intType()
            .noDefaultValue()
            .withDescription("The size of window.");
    Option<Boolean> MINI_BATCH = Options.key("mini_batch")
            .booleanType()
            .defaultValue(false)
            .withDescription("The label of pre_aggrergate.");
    Option<Integer> WINDOW_SLIDE = Options.key("window_slide")
            .intType()
            .noDefaultValue()
            .withDescription("The size of sliding window.");

    Option<String> WINDOW_TIMESTAMP_FIELD = Options.key("window_timestamp_field")
            .stringType()
            .noDefaultValue()
            .withDescription("which field to be set the start time of window.");
}
