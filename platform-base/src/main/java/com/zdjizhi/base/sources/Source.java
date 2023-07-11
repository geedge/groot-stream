package com.zdjizhi.base.sources;

import com.zdjizhi.base.pojo.GrootEvent;
import com.zdjizhi.base.pojo.SourceConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface Source {

    SingleOutputStreamOperator<GrootEvent> source(StreamExecutionEnvironment env , SourceConfig sourceConfig) throws Exception;

}
