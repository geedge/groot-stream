package com.geedgenetworks.spi.filter;

import com.geedgenetworks.spi.table.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.Map;

public interface Filter<T extends FilterConfig> extends Serializable {

    DataStream<Event> filterFunction(
            DataStream<Event> singleOutputStreamOperator, T FilterConfig)
            throws Exception;
    String type();

    T checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig);

}
