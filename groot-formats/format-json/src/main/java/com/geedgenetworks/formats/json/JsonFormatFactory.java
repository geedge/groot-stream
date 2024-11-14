package com.geedgenetworks.formats.json;

import com.geedgenetworks.api.connector.serialization.DecodingFormat;
import com.geedgenetworks.api.connector.serialization.EncodingFormat;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.factory.DecodingFormatFactory;
import com.geedgenetworks.api.factory.EncodingFormatFactory;
import com.geedgenetworks.api.factory.ConnectorFactory;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.geedgenetworks.formats.json.JsonFormatOptions.IGNORE_PARSE_ERRORS;

public class JsonFormatFactory implements DecodingFormatFactory, EncodingFormatFactory {
    public static final String IDENTIFIER = "json";

    @Override
    public String type() {
        return IDENTIFIER;
    }

    @Override
    public boolean supportsType(String type) {
        return IDENTIFIER.equalsIgnoreCase(type);
    }

    @Override
    public DecodingFormat createDecodingFormat(ConnectorFactory.Context context, ReadableConfig formatOptions) {
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        return new DecodingFormat(){
            @Override
            public DeserializationSchema<Event> createRuntimeDecoder(StructType dataType) {
                return new JsonEventDeserializationSchema(dataType, ignoreParseErrors);
            }
        };

    }

    @Override
    public EncodingFormat createEncodingFormat(ConnectorFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat() {
            @Override
            public SerializationSchema<Event> createRuntimeEncoder(StructType dataType) {
                return new JsonEventSerializationSchema(dataType);
            }
        };
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IGNORE_PARSE_ERRORS);
        return options;
    }

}
