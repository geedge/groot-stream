package com.geedgenetworks.core.processor.aggregate;

import cn.hutool.crypto.SecureUtil;
import com.geedgenetworks.common.config.KeybyEntity;
import com.geedgenetworks.api.event.Event;

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
        for (String key : keys) {
            Object object = value.getExtractedFields().get(key);
            if(object==null){
                stringBuilder.append(",");
            }else{
                keybyEntity.getKeys().put(key, object);
                stringBuilder.append(object);
            }
        }
        String hashedKey = SecureUtil.md5(stringBuilder.toString());
        keybyEntity.setKeysToString(hashedKey);
        return keybyEntity;
    }
}
