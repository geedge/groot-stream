package com.geedgenetworks.formats.json;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.connector.format.DecodingFormat;
import com.geedgenetworks.core.connector.format.EncodingFormat;
import com.geedgenetworks.core.factories.DecodingFormatFactory;
import com.geedgenetworks.core.factories.EncodingFormatFactory;
import com.geedgenetworks.core.factories.TableFactory;
import com.geedgenetworks.core.types.StructType;
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
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DecodingFormat createDecodingFormat(TableFactory.Context context, ReadableConfig formatOptions) {
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        return new DecodingFormat(){
            @Override
            public DeserializationSchema<Event> createRuntimeDecoder(StructType dataType) {
                return new JsonEventDeserializationSchema(dataType, ignoreParseErrors);
            }
        };

    }

    @Override
    public EncodingFormat createEncodingFormat(TableFactory.Context context, ReadableConfig formatOptions) {
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
