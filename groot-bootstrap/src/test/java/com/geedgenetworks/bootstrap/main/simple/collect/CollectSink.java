package com.geedgenetworks.bootstrap.main.simple.collect;

import com.geedgenetworks.common.Event;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.*;

public  class CollectSink implements SinkFunction<Event> {

    // must be static
    public static final List<Event> values = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(Event value, Context context) throws Exception {
        values.add(value);
    }
}
