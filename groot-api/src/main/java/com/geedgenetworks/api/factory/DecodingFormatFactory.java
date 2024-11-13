package com.geedgenetworks.api.factory;

import com.geedgenetworks.api.connector.serialization.DecodingFormat;
import org.apache.flink.configuration.ReadableConfig;

public interface DecodingFormatFactory extends FormatFactory {
    DecodingFormat createDecodingFormat(ConnectorFactory.Context context, ReadableConfig formatOptions);
}


