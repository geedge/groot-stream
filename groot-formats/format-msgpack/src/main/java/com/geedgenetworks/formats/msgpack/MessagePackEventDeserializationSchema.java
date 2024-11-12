package com.geedgenetworks.formats.msgpack;


import com.geedgenetworks.api.connector.serialization.MapDeserialization;
import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.Map;

public class MessagePackEventDeserializationSchema implements DeserializationSchema<Event>, MapDeserialization {
    private final StructType dataType;
    private final MessagePackDeserializer deserializer;

    public MessagePackEventDeserializationSchema(StructType dataType) {
        this.dataType = dataType;
        this.deserializer = new MessagePackDeserializer(dataType);
    }

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        try {
            Map<String, Object> map = deserializer.deserialize(bytes);
            Event event = new Event();
            event.setExtractedFields(map);
            return event;
        } catch (Exception e) {
            throw new IOException(StringUtils.byteToHexString(bytes), e);
        }
    }

    @Override
    public Map<String, Object> deserializeToMap(byte[] bytes) throws IOException {
        try {
            return deserializer.deserialize(bytes);
        } catch (Exception e) {
            throw new IOException(StringUtils.byteToHexString(bytes), e);
        }
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return null;
    }
}
