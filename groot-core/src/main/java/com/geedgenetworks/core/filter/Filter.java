package com.geedgenetworks.core.filter;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.FilterConfig;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Filter {

    DataStream<Event> filterFunction(
            DataStream<Event> singleOutputStreamOperator, FilterConfig FilterConfig)
            throws Exception;
    String type();
}
