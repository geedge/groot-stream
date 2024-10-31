package com.geedgenetworks.formats.csv;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class CsvFormatOptions {
    public static final ConfigOption<String> FIELD_DELIMITER = ConfigOptions.key("field.delimiter")
            .stringType()
            .defaultValue(",")
            .withDescription("Optional field delimiter character (',' by default)");
    
    public static final ConfigOption<Boolean> DISABLE_QUOTE_CHARACTER = ConfigOptions.key("disable.quote.character")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to disabled quote character for enclosing field values (false by default)\n"
                                    + "if true, quote-character can not be set");

    public static final ConfigOption<String> QUOTE_CHARACTER = ConfigOptions.key("quote.character")
                    .stringType()
                    .defaultValue("\"")
                    .withDescription(
                            "Optional quote character for enclosing field values ('\"' by default)");

    public static final ConfigOption<Boolean> ALLOW_COMMENTS = ConfigOptions.key("allow.comments")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to ignore comment lines that start with \"#\"\n"
                                    + "(disabled by default);\n"
                                    + "if enabled, make sure to also ignore parse errors to allow empty rows");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions.key("ignore.parse.errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors");

    public static final ConfigOption<String> ARRAY_ELEMENT_DELIMITER = ConfigOptions.key("array.element.delimiter")
                    .stringType()
                    .defaultValue(";")
                    .withDescription(
                            "Optional array element delimiter string for separating\n"
                                    + "array and row element values (\";\" by default)");

    public static final ConfigOption<String> ESCAPE_CHARACTER = ConfigOptions.key("escape.character")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional escape character for escaping values (disabled by default)");

    public static final ConfigOption<String> NULL_LITERAL = ConfigOptions.key("null.literal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional null literal string that is interpreted as a\n"
                                    + "null value (disabled by default)");

}
