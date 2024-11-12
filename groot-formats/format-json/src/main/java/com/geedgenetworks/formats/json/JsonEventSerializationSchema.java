package com.geedgenetworks.formats.json;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.filter.PropertyFilter;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;
import com.geedgenetworks.api.connector.event.Event;

public class JsonEventSerializationSchema implements SerializationSchema<Event> {
    // __开头字段为内部字段，过滤掉
    private static PropertyFilter proFilter = new PropertyFilter() {
        @Override
        public boolean apply(Object object, String name, Object value) {
            if (name.startsWith("__")) {
                return false;
            }
            return true;
        }
    };
    private final StructType dataType;
    private final JsonSerializer serializer;

    public JsonEventSerializationSchema(StructType dataType) {
        this.dataType = dataType;
        this.serializer = dataType != null? new JsonSerializer(dataType): null;
    }

    @Override
    public byte[] serialize(Event element) {
        if(dataType == null){
            return JSON.toJSONBytes(element.getExtractedFields(), proFilter);
        } else {
            return serializer.serialize(element.getExtractedFields());
        }
    }
}
