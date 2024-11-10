package com.geedgenetworks.spi.processor;

import com.geedgenetworks.spi.table.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Map;

public interface Processor<T extends ProcessorConfig> extends Serializable {

    DataStream<Event> process(StreamExecutionEnvironment env, DataStream<Event> input, T processorConfig) ;

    String type();

    T checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig);
}
