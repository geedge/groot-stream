package net.geedgenetworks.source;

import net.geedgenetworks.pojo.GrootEvent;
import net.geedgenetworks.pojo.SourceConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface Source {

    SingleOutputStreamOperator<GrootEvent> source(StreamExecutionEnvironment env , SourceConfig sourceConfig) throws Exception;

}
