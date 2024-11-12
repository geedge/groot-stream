package com.geedgenetworks.api.connector.source;

import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

public interface SourceProvider extends Serializable {
    SingleOutputStreamOperator<Event> produceDataStream(StreamExecutionEnvironment env);

    StructType getPhysicalDataType();

    default StructType schema(){
        return getPhysicalDataType();
    }

    default boolean supportDynamicSchema(){
        return false;
    }
}
