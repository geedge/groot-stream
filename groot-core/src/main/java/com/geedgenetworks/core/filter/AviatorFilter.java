package com.geedgenetworks.core.filter;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.FilterConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class AviatorFilter implements Filter {

    @Override
    public DataStream<Event> filterFunction(
            DataStream<Event> singleOutputStreamOperator, FilterConfig FilterConfig)
            throws Exception {

        if (FilterConfig.getParallelism() != 0) {
            return singleOutputStreamOperator
                    .filter(new FilterFunction(FilterConfig))
                    .setParallelism(FilterConfig.getParallelism())
                    .name(FilterConfig.getName());
        } else {
            return singleOutputStreamOperator
                    .filter(new FilterFunction(FilterConfig))
                    .name(FilterConfig.getName());
        }
    }

    @Override
    public String type() {
        return "aviator";
    }

}
