package com.geedgenetworks.connectors.kafka.sink;

import com.geedgenetworks.connectors.kafka.util.KafkaUtils;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.SinkConfig;
import com.geedgenetworks.core.sink.Sink;
import com.geedgenetworks.core.utils.ColumnUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import com.alibaba.fastjson2.JSON;

public class KafkaSink implements Sink {

    @Override
    public void sink(
            SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator,
            SinkConfig sinkConfig)
            throws Exception {
        if (sinkConfig.getParallelism() != 0) {
            streamlineEventSingleOutputStreamOperator
                    .map(
                            new MapFunction<Event, String>() {
                                @Override
                                public String map(Event value) throws Exception {
                                    if (sinkConfig.getOutput_fields() != null
                                            && sinkConfig.getOutput_fields().size() > 0) {
                                        value.setExtractedFields(
                                                ColumnUtil.columnSelector(
                                                        value.getExtractedFields(),
                                                        sinkConfig.getOutput_fields()));
                                    }
                                    String jsonString =
                                            JSON.toJSONString(value.getExtractedFields());
                                    return jsonString;
                                }
                            })
                    .setParallelism(sinkConfig.getParallelism())
                    .addSink(KafkaUtils.getKafkaSink(sinkConfig.getProperties()))
                    .setParallelism(sinkConfig.getParallelism())
                    .name(sinkConfig.getName());
        } else {

            streamlineEventSingleOutputStreamOperator
                    .map(
                            new MapFunction<Event, String>() {
                                @Override
                                public String map(Event value) throws Exception {
                                    if (sinkConfig.getOutput_fields() != null
                                            && sinkConfig.getOutput_fields().size() > 0) {
                                        value.setExtractedFields(
                                                ColumnUtil.columnSelector(
                                                        value.getExtractedFields(),
                                                        sinkConfig.getOutput_fields()));
                                    }
                                    String jsonString =
                                            JSON.toJSONString(value.getExtractedFields());
                                    return jsonString;
                                }
                            })
                    .addSink(KafkaUtils.getKafkaSink(sinkConfig.getProperties()))
                    .name(sinkConfig.getName());
        }
    }
}
