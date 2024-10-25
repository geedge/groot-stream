package com.geedgenetworks.core.split;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.SplitConfig;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface Split<T extends SplitConfig> extends Serializable {

    DataStream<Event> splitFunction(
            DataStream<Event> dataStream, T splitConfig)
            throws Exception;
    String type();

    T checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig);

}
