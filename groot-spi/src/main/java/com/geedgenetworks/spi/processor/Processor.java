package com.geedgenetworks.spi.processor;

import com.geedgenetworks.spi.table.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.Map;

public interface Processor<T extends ProcessorConfig> extends Serializable {

    DataStream<Event> processorFunction(
            DataStream<Event> singleOutputStreamOperator,
            T processorConfig, ExecutionConfig config) ;

    String type();
    T checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig);
}
