package com.geedgenetworks.api.connector.serialization;

import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;

public interface DecodingFormat {
    DeserializationSchema<Event> createRuntimeDecoder(StructType dataType);
}
