package com.geedgenetworks.formats.raw;


import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import com.geedgenetworks.api.connector.type.Types;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.Preconditions;

public class RawEventSerializationSchema implements SerializationSchema<Event> {
    private final StructType dataType;
    private final String name;

    public RawEventSerializationSchema(StructType dataType) {
        Preconditions.checkArgument(dataType.fields.length == 1 && dataType.fields[0].dataType.equals(Types.BINARY), "must is one binary type field");
        this.dataType = dataType;
        this.name = dataType.fields[0].name;
    }

    @Override
    public byte[] serialize(Event element) {
        byte[] data = (byte[])element.getExtractedFields().get(name);
        return data;
    }

}
