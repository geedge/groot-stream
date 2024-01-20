package com.geedgenetworks.core.connector.format;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;

public interface EncodingFormat {
    SerializationSchema<Event> createRuntimeEncoder(StructType dataType);

}
