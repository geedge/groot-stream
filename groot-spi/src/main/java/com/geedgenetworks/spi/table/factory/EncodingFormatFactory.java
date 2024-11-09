package com.geedgenetworks.spi.table.factory;

import com.geedgenetworks.spi.table.connector.EncodingFormat;
import org.apache.flink.configuration.ReadableConfig;

public interface EncodingFormatFactory extends FormatFactory {
    EncodingFormat createEncodingFormat(TableFactory.Context context, ReadableConfig formatOptions);
}
