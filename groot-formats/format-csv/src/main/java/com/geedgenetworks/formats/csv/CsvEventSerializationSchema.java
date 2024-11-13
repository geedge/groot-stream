package com.geedgenetworks.formats.csv;

import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

public class CsvEventSerializationSchema implements SerializationSchema<Event> {
    private final StructType dataType;
    private final CsvSchema csvSchema;
    private final CsvSerializer serializer;

    public CsvEventSerializationSchema(StructType dataType, CsvSchema csvSchema) {
        this.dataType = dataType;
        this.csvSchema = csvSchema.withLineSeparator("");
        this.serializer = new CsvSerializer(dataType, this.csvSchema);
    }

    @Override
    public byte[] serialize(Event element) {
        try {
            return serializer.serialize(element.getExtractedFields());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
