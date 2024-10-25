package com.geedgenetworks.core.processor;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.ProcessorConfig;
import com.typesafe.config.Config;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.Serializable;
import java.util.Map;

public interface Processor<T extends ProcessorConfig> extends Serializable {

    DataStream<Event> processorFunction(
            DataStream<Event> singleOutputStreamOperator,
            T processorConfig, ExecutionConfig config)
            throws Exception;
    String type();

     T checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig);
}
