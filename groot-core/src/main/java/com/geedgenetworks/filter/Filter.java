package com.geedgenetworks.filter;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.FilterConfig;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Filter {

    SingleOutputStreamOperator<Event> filterFunction(
            SingleOutputStreamOperator<Event> singleOutputStreamOperator, FilterConfig FilterConfig)
            throws Exception;
}
