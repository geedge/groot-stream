package com.geedgenetworks.filter;


import com.geedgenetworks.pojo.FilterEntity;
import com.geedgenetworks.pojo.GrootEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Filter {

    SingleOutputStreamOperator<GrootEvent> filterFunction(SingleOutputStreamOperator<GrootEvent> singleOutputStreamOperator, FilterEntity FilterEntity) throws Exception;
}
