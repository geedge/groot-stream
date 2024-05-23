package com.geedgenetworks.connectors.file;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class FileConnectorOptions {
    public static final ConfigOption<String> PATH =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("file path");

    public static final ConfigOption<Integer> ROWS_PER_SECOND =
            ConfigOptions.key("rows.per.second")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Rows per second to control the emit rate.");

    public static final ConfigOption<Long> NUMBER_OF_ROWS =
            ConfigOptions.key("number.of.rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("Total number of rows to emit. By default, the source is unbounded.");

    public static final ConfigOption<Long> MILLIS_PER_ROW =
            ConfigOptions.key("millis.per.row")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("millis per row to control the emit rate.");

    public static final ConfigOption<Boolean> READ_LOCAL_FILE_IN_CLIENT =
            ConfigOptions.key("read.local.file.in.client")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether read local file in client.");

}
