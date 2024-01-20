package com.geedgenetworks.core.connector.source;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

public interface SourceProvider extends Serializable {
    SingleOutputStreamOperator<Event> produceDataStream(StreamExecutionEnvironment env);

    StructType getPhysicalDataType();

    default StructType schema(){
        return getPhysicalDataType();
    }
}
