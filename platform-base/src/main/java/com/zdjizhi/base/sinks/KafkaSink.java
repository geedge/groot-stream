package com.zdjizhi.base.sinks;

import com.alibaba.fastjson.JSON;
import com.zdjizhi.base.pojo.GrootEvent;
import com.zdjizhi.base.pojo.SinkConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import static com.zdjizhi.base.utils.KafkaUtils.getKafkaSink;

public class KafkaSink implements Sink{




    @Override
    public void sink(SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator, SinkConfig sinkConfig) throws Exception {
        streamlineEventSingleOutputStreamOperator.map(new MapFunction<GrootEvent, String>() {
            @Override
            public String map(GrootEvent value) throws Exception {

                String jsonString = JSON.toJSONString(value.getAuxiliaryFieldsAndValues());
                return jsonString;
            }
        }).setParallelism(sinkConfig.getParallelism()).addSink( getKafkaSink(sinkConfig.getKafka())).setParallelism(sinkConfig.getParallelism());
    }
}
