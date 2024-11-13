package com.geedgenetworks.api.processor;

import com.geedgenetworks.api.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.io.Serializable;


public interface Processor<T extends ProcessorConfig> extends Serializable {

    DataStream<Event> process(StreamExecutionEnvironment env, DataStream<Event> input, T processorConfig) ;

    T parseConfig(String name, Config config);
}
