package com.geedgenetworks.formats.json;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class JsonFormatOptions {
    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore.parse.errors")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, true by default.");

}
