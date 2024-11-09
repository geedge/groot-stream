package com.geedgenetworks.spi.table.factory;

import com.geedgenetworks.spi.table.connector.DecodingFormat;
import org.apache.flink.configuration.ReadableConfig;

public interface DecodingFormatFactory extends FormatFactory {
    DecodingFormat createDecodingFormat(TableFactory.Context context, ReadableConfig formatOptions);
}


