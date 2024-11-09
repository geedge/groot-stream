package com.geedgenetworks.spi.split;

import com.geedgenetworks.spi.table.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.Map;

public interface Split<T extends SplitConfig> extends Serializable {

    DataStream<Event> splitFunction(
            DataStream<Event> dataStream, T splitConfig)
            throws Exception;
    String type();

    T checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig);

}
