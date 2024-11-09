package com.geedgenetworks.core.filter;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.spi.filter.Filter;
import com.geedgenetworks.spi.filter.FilterConfig;
import com.geedgenetworks.spi.table.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Map;

public class AviatorFilter implements Filter<FilterConfig> {

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

    @Override
    public FilterConfig checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig) {

        FilterConfig filterConfig = new JSONObject(configProperties).toJavaObject(FilterConfig.class);
        filterConfig.setName(name);
        return filterConfig;
    }

}
