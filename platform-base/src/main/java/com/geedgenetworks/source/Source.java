package com.geedgenetworks.source;

import com.geedgenetworks.pojo.SourceConfig;
import com.geedgenetworks.pojo.GrootEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface Source {

    SingleOutputStreamOperator<GrootEvent> source(StreamExecutionEnvironment env , SourceConfig sourceConfig) throws Exception;

}
