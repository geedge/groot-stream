package com.geedgenetworks.spi.table.connector;

import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.type.StructType;
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
