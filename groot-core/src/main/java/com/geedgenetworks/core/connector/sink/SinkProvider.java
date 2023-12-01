package com.geedgenetworks.core.connector.sink;

import com.geedgenetworks.core.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.io.Serializable;

public interface SinkProvider extends Serializable {
    DataStreamSink<?>  consumeDataStream(DataStream<Event> dataStream);
}
