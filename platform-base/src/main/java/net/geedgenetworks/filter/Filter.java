package net.geedgenetworks.filter;


import net.geedgenetworks.pojo.GrootEvent;
import net.geedgenetworks.pojo.FilterEntity;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface Filter {

    SingleOutputStreamOperator<GrootEvent> filterFunction(SingleOutputStreamOperator<GrootEvent> singleOutputStreamOperator, FilterEntity FilterEntity) throws Exception;
}
