package com.geedgenetworks.core.filter;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.FilterConfig;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Filter {

    SingleOutputStreamOperator<Event> filterFunction(
            SingleOutputStreamOperator<Event> singleOutputStreamOperator, FilterConfig FilterConfig)
            throws Exception;
}
