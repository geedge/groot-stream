package com.geedgenetworks.core.factories;

import com.geedgenetworks.core.connector.format.EncodingFormat;
import org.apache.flink.configuration.ReadableConfig;

public interface EncodingFormatFactory extends FormatFactory {
    EncodingFormat createEncodingFormat(TableFactory.Context context, ReadableConfig formatOptions);
}
