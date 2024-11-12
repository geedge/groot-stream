package com.geedgenetworks.api.factory;

import com.geedgenetworks.api.connector.serialization.EncodingFormat;
import org.apache.flink.configuration.ReadableConfig;

public interface EncodingFormatFactory extends FormatFactory {
    EncodingFormat createEncodingFormat(ConnectorFactory.Context context, ReadableConfig formatOptions);
}
