package com.geedgenetworks.core.filter;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.FilterConfig;

import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.Serializable;
import java.util.Map;

public interface Filter<T extends FilterConfig> extends Serializable {

    DataStream<Event> filterFunction(
            DataStream<Event> singleOutputStreamOperator, T FilterConfig)
            throws Exception;
    String type();

    T checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig);

}
