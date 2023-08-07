package com.geedgenetworks.common;

import com.geedgenetworks.pojo.GrootEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;

import java.util.List;

public class OneKeySelector implements KeySelector<GrootEvent, Tuple1<Object>> {

    private List<String> keys;

    public OneKeySelector(List<String> keys) {
        this.keys=keys;
    }

    @Override
    public Tuple1<Object> getKey(GrootEvent value) throws Exception {
        return new Tuple1<>(value.getAuxiliaryFieldsAndValues().get(keys.get(0)));

    }
}
