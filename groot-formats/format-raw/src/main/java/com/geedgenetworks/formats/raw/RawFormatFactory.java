package com.geedgenetworks.formats.raw;

import com.geedgenetworks.spi.table.connector.DecodingFormat;
import com.geedgenetworks.spi.table.connector.EncodingFormat;
import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.factory.DecodingFormatFactory;
import com.geedgenetworks.spi.table.factory.EncodingFormatFactory;
import com.geedgenetworks.spi.table.factory.TableFactory;
import com.geedgenetworks.spi.table.type.StructType;
import com.geedgenetworks.spi.table.type.StructType.StructField;
import com.geedgenetworks.spi.table.type.Types;
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
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DecodingFormat createDecodingFormat(TableFactory.Context context, ReadableConfig formatOptions) {
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
    public EncodingFormat createEncodingFormat(TableFactory.Context context, ReadableConfig formatOptions) {
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
