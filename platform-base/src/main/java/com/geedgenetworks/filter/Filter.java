package com.geedgenetworks.filter;


import com.geedgenetworks.pojo.FilterEntity;
import com.geedgenetworks.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Filter {

    SingleOutputStreamOperator<Event> filterFunction(SingleOutputStreamOperator<Event> singleOutputStreamOperator, FilterEntity FilterEntity) throws Exception;
}
