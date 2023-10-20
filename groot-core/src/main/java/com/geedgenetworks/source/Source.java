package com.geedgenetworks.source;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.SourceConfig;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface Source {

    SingleOutputStreamOperator<Event> source(
            StreamExecutionEnvironment env, SourceConfig sourceConfig) throws Exception;
}
