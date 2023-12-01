package com.geedgenetworks.core.factories;


import com.geedgenetworks.core.connector.format.DecodingFormat;
import org.apache.flink.configuration.ReadableConfig;

public interface DecodingFormatFactory extends FormatFactory {
    DecodingFormat createDecodingFormat(TableFactory.Context context, ReadableConfig formatOptions);
}
