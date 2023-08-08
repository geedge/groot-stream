package com.geedgenetworks.sink;

import com.alibaba.fastjson.JSON;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.SinkConfig;
import com.geedgenetworks.utils.KafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class KafkaSink implements Sink{




    @Override
    public void sink(SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator, SinkConfig sinkConfig) throws Exception {
        streamlineEventSingleOutputStreamOperator.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {

                String jsonString = JSON.toJSONString(value.getExtractedFields());
                return jsonString;
            }
        }).setParallelism(sinkConfig.getParallelism()).addSink( KafkaUtils.getKafkaSink(sinkConfig.getProperties())).setParallelism(sinkConfig.getParallelism());
    }
}
