package com.geedgenetworks.formats.json;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonEventDeserializationSchema implements DeserializationSchema<Event> {
    private final StructType dataType;
    private final boolean ignoreParseErrors;
    private final JsonToMapDataConverter converter;

    public JsonEventDeserializationSchema(StructType dataType, boolean ignoreParseErrors) {
        this.dataType = dataType;
        this.ignoreParseErrors = ignoreParseErrors;
        this.converter = dataType != null ?new JsonToMapDataConverter(dataType, ignoreParseErrors):null;
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        Map<String, Object> map;

        // 没有配置type, 不进行类型校验和转换
        if(dataType == null){
            try {
                map = JSON.parseObject(message);
            } catch (Exception e) {
                if(ignoreParseErrors){
                    map = new HashMap<>();
                }else{
                    throw new IOException(e);
                }
            }

        }else{
            // 进行类型校验, 类型不一致时会尝试进行类型转换
            map = converter.convert(message);
        }

        Event event = new Event();
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
