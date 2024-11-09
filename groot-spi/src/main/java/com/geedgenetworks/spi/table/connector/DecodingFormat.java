package com.geedgenetworks.spi.table.connector;

import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;

public interface DecodingFormat {
    DeserializationSchema<Event> createRuntimeDecoder(StructType dataType);
}
