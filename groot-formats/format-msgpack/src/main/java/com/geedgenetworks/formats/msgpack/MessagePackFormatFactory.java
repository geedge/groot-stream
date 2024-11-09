package com.geedgenetworks.formats.msgpack;


import com.geedgenetworks.spi.table.connector.DecodingFormat;
import com.geedgenetworks.spi.table.connector.EncodingFormat;
import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.factory.DecodingFormatFactory;
import com.geedgenetworks.spi.table.factory.EncodingFormatFactory;
import com.geedgenetworks.spi.table.factory.TableFactory;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Collections;
import java.util.Set;

public class MessagePackFormatFactory implements DecodingFormatFactory, EncodingFormatFactory {
    public static final String IDENTIFIER = "msgpack";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DecodingFormat createDecodingFormat(TableFactory.Context context, ReadableConfig formatOptions) {

        return new DecodingFormat() {
            @Override
            public DeserializationSchema<Event> createRuntimeDecoder(StructType dataType) {
                return new MessagePackEventDeserializationSchema(dataType);
            }
        };
    }

    @Override
    public EncodingFormat createEncodingFormat(TableFactory.Context context, ReadableConfig formatOptions) {

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
