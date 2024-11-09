package com.geedgenetworks.spi.table.connector;

import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;

public interface EncodingFormat {
    SerializationSchema<Event> createRuntimeEncoder(StructType dataType);

}
