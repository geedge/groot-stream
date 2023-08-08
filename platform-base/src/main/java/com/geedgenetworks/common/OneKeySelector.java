package com.geedgenetworks.common;

import com.geedgenetworks.pojo.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;

import java.util.List;

public class OneKeySelector implements KeySelector<Event, Tuple1<Object>> {

    private List<String> keys;

    public OneKeySelector(List<String> keys) {
        this.keys=keys;
    }

    @Override
    public Tuple1<Object> getKey(Event value) throws Exception {
        return new Tuple1<>(value.getExtractedFields().get(keys.get(0)));

    }
}
