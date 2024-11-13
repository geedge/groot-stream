package com.geedgenetworks.formats.raw;

import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import com.geedgenetworks.api.connector.type.Types;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RawEventDeserializationSchema implements DeserializationSchema<Event> {
    private final StructType dataType;
    private final String name;

    public RawEventDeserializationSchema(StructType dataType) {
        Preconditions.checkArgument(dataType.fields.length == 1 && dataType.fields[0].dataType.equals(Types.BINARY), "must is one binary type field");
        this.dataType = dataType;
        this.name = dataType.fields[0].name;
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        Event event = new Event();
        Map<String, Object> map = new HashMap<>(8);
        map.put(name, message);
        event.setExtractedFields(map);
        return event;
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
