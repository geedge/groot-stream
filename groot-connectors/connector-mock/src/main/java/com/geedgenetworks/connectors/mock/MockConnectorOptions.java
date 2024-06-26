package com.geedgenetworks.connectors.mock;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class MockConnectorOptions {

    public static final ConfigOption<String> MOCK_DESC_FILE_PATH =
            ConfigOptions.key("mock.desc.file.path")
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

}
