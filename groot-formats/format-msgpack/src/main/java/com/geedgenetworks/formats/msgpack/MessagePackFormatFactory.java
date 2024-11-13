package com.geedgenetworks.formats.msgpack;


import com.geedgenetworks.api.connector.serialization.DecodingFormat;
import com.geedgenetworks.api.connector.serialization.EncodingFormat;
import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.factory.DecodingFormatFactory;
import com.geedgenetworks.api.factory.EncodingFormatFactory;
import com.geedgenetworks.api.factory.ConnectorFactory;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Collections;
import java.util.Set;

public class MessagePackFormatFactory implements DecodingFormatFactory, EncodingFormatFactory {
    public static final String IDENTIFIER = "msgpack";

    @Override
    public String type() {
        return IDENTIFIER;
    }

    @Override
    public DecodingFormat createDecodingFormat(ConnectorFactory.Context context, ReadableConfig formatOptions) {

        return new DecodingFormat() {
            @Override
            public DeserializationSchema<Event> createRuntimeDecoder(StructType dataType) {
                return new MessagePackEventDeserializationSchema(dataType);
            }
        };
    }

    @Override
    public EncodingFormat createEncodingFormat(ConnectorFactory.Context context, ReadableConfig formatOptions) {

        return new EncodingFormat() {
            @Override
            public SerializationSchema<Event> createRuntimeEncoder(StructType dataType) {
                return new MessagePackEventSerializationSchema(dataType);
            }
        };
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
