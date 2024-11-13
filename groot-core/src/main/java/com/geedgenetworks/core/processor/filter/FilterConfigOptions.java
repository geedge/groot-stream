package com.geedgenetworks.core.processor.filter;

import com.geedgenetworks.common.config.Option;
import com.geedgenetworks.common.config.Options;

import java.util.List;
import java.util.Map;

public interface FilterConfigOptions {
    Option<List<String>> OUTPUT_FIELDS = Options.key("output_fields")
            .listType()
            .noDefaultValue()
            .withDescription("The fields to be outputted.");

}
