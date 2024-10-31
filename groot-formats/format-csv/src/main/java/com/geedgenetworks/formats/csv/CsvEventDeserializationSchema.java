package com.geedgenetworks.formats.csv;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.connector.format.MapDeserialization;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CsvEventDeserializationSchema implements DeserializationSchema<Event>, MapDeserialization {
    private final StructType dataType;
    private final CsvSchema csvSchema;
    private final boolean ignoreParseErrors;
    private final CsvToMapDataConverter converter;

    public CsvEventDeserializationSchema(StructType dataType, CsvSchema csvSchema, boolean ignoreParseErrors) {
        this.dataType = dataType;
        this.csvSchema = csvSchema;
        this.ignoreParseErrors = ignoreParseErrors;
        this.converter = new CsvToMapDataConverter(dataType, csvSchema, ignoreParseErrors);
    }

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        Map<String, Object> map = deserializeToMap(bytes);
        if (map == null) {
            return null;
        }
        Event event = new Event();
        event.setExtractedFields(map);
        return event;
    }

    @Override
    public Map<String, Object> deserializeToMap(byte[] bytes) throws IOException {
        String message = new String(bytes, StandardCharsets.UTF_8);
        return converter.convert(message);
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
