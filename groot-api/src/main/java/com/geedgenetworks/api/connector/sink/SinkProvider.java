package com.geedgenetworks.api.connector.sink;

import com.geedgenetworks.api.event.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.io.Serializable;

public interface SinkProvider extends Serializable {
    DataStreamSink<?>  consumeDataStream(DataStream<Event> dataStream);

    default boolean supportDynamicSchema(){
        return false;
    }
}
