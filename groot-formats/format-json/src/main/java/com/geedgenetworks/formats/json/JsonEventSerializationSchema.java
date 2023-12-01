package com.geedgenetworks.formats.json;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.filter.PropertyFilter;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;

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

    public JsonEventSerializationSchema(StructType dataType) {
        this.dataType = dataType;
    }

    @Override
    public byte[] serialize(Event element) {
        // sink暂不支持类型推断, dataType为null
        return JSON.toJSONBytes(element.getExtractedFields(), proFilter);
    }
}
