package com.geedgenetworks.core.filter;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.FilterConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class AviatorFilter implements Filter {

    @Override
    public SingleOutputStreamOperator<Event> filterFunction(
            SingleOutputStreamOperator<Event> singleOutputStreamOperator, FilterConfig FilterConfig)
            throws Exception {

        if (FilterConfig.getParallelism() != 0) {
            return singleOutputStreamOperator
                    .filter(new FilterFunction(FilterConfig))
                    .setParallelism(FilterConfig.getParallelism())
                    .name("Filter");
        } else {
            return singleOutputStreamOperator
                    .filter(new FilterFunction(FilterConfig))
                    .name("Filter");
        }
    }
}
