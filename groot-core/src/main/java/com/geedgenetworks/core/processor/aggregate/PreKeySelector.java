package com.geedgenetworks.core.processor.aggregate;

import cn.hutool.crypto.SecureUtil;
import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.KeybyEntity;

import java.util.HashMap;
import java.util.List;

public class PreKeySelector implements org.apache.flink.api.java.functions.KeySelector<Accumulator, KeybyEntity> {


    private final List<String> keys;

    public PreKeySelector(List<String> keys) {
        this.keys = keys;
    }

    @Override
    public KeybyEntity getKey(Accumulator value) throws Exception {

        KeybyEntity keybyEntity = new KeybyEntity(new HashMap<>());
        StringBuilder stringBuilder = new StringBuilder();
        for (String key : keys) {

            if (value.getMetricsFields().containsKey(key)) {
                keybyEntity.getKeys().put(key, value.getMetricsFields().get(key));
                stringBuilder.append(value.getMetricsFields().get(key).toString());
            } else {
                stringBuilder.append(",");
            }
        }
        String hashedKey = SecureUtil.md5(stringBuilder.toString());
        keybyEntity.setKeysToString(hashedKey);
        return keybyEntity;
    }
}
