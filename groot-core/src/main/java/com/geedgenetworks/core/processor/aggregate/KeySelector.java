package com.geedgenetworks.core.processor.aggregate;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.KeybyEntity;

import java.util.HashMap;
import java.util.List;

public class KeySelector implements org.apache.flink.api.java.functions.KeySelector<Event, KeybyEntity> {


    private final List<String> keys;

    public KeySelector(List<String> keys) {
        this.keys = keys;
    }

    @Override
    public KeybyEntity getKey(Event value) throws Exception {

        KeybyEntity keybyEntity = new KeybyEntity(new HashMap<>());
        StringBuilder stringBuilder = new StringBuilder();
        for(String key: keys){

            if(value.getExtractedFields().containsKey(key)){
                keybyEntity.getKeys().put(key,value.getExtractedFields().get(key));
                stringBuilder.append(value.getExtractedFields().get(key).toString());
            }else {
                stringBuilder.append(",");
            }
        }
        keybyEntity.setKeysToString(stringBuilder.toString());
        return  keybyEntity;
    }
}
