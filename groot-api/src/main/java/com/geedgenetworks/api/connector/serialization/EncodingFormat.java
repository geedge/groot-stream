package com.geedgenetworks.api.connector.serialization;

import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;

public interface EncodingFormat {
    SerializationSchema<Event> createRuntimeEncoder(StructType dataType);

}
