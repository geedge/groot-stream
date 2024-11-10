package com.geedgenetworks.spi.sink;

import com.geedgenetworks.spi.table.event.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.io.Serializable;

public interface SinkProvider extends Serializable {
    DataStreamSink<?>  consumeDataStream(DataStream<Event> dataStream);

    default boolean supportDynamicSchema(){
        return false;
    }
}
