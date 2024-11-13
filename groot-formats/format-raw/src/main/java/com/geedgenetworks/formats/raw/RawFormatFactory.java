package com.geedgenetworks.formats.raw;

import com.geedgenetworks.api.connector.serialization.DecodingFormat;
import com.geedgenetworks.api.connector.serialization.EncodingFormat;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.factory.DecodingFormatFactory;
import com.geedgenetworks.api.factory.EncodingFormatFactory;
import com.geedgenetworks.api.factory.ConnectorFactory;
import com.geedgenetworks.api.connector.type.StructType;
import com.geedgenetworks.api.connector.type.StructType.StructField;
import com.geedgenetworks.api.connector.type.Types;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Collections;
import java.util.Set;

public class RawFormatFactory implements DecodingFormatFactory, EncodingFormatFactory {
    public static final String IDENTIFIER = "raw";
    public static final StructType DEFAULT_DATATYPE = new StructType(new StructField[]{new StructField("raw", Types.BINARY)});

    @Override
    public String type() {
        return IDENTIFIER;
    }

    @Override
    public DecodingFormat createDecodingFormat(ConnectorFactory.Context context, ReadableConfig formatOptions) {
        return new DecodingFormat(){
            @Override
            public DeserializationSchema<Event> createRuntimeDecoder(StructType dataType) {
                if(dataType == null){
                    dataType = DEFAULT_DATATYPE;
                }
                return new RawEventDeserializationSchema(dataType);
            }
        };

    }

    @Override
    public EncodingFormat createEncodingFormat(ConnectorFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat() {
            @Override
            public SerializationSchema<Event> createRuntimeEncoder(StructType dataType) {
                if(dataType == null){
                    dataType = DEFAULT_DATATYPE;
                }
                return new RawEventSerializationSchema(dataType);
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
