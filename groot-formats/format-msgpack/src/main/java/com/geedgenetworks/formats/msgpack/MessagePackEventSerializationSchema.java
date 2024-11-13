package com.geedgenetworks.formats.msgpack;

import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class MessagePackEventSerializationSchema implements SerializationSchema<Event> {
    private final StructType dataType;
    private final MessagePackSerializer serializer;

    public MessagePackEventSerializationSchema(StructType dataType) {
        this.dataType = dataType;
        this.serializer = new MessagePackSerializer(dataType);
    }

    @Override
    public byte[] serialize(Event element) {
        return serializer.serialize(element.getExtractedFields());
    }
}
