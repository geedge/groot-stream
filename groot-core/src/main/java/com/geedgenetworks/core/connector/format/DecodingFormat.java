package com.geedgenetworks.core.connector.format;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;

public interface DecodingFormat {
    DeserializationSchema<Event> createRuntimeDecoder(StructType dataType);
}
