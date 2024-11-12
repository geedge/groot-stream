package com.geedgenetworks.formats.protobuf;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.api.connector.sink.SinkProvider;
import com.geedgenetworks.api.connector.sink.SinkTableFactory;
import com.geedgenetworks.api.connector.source.SourceProvider;
import com.geedgenetworks.api.connector.source.SourceTableFactory;
import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.factory.FactoryUtil;
import com.geedgenetworks.api.factory.ConnectorFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;


class ProtobufFormatFactoryTest {

    public static void main(String[] args) throws Exception{
        ProtobufEventSchemaTest.InputDatas inputDatas = ProtobufEventSchemaTest.geneInputDatasUsePartialField();
        String path = ProtobufFormatFactoryTest.class.getResource("/proto3_types.desc").getPath();
        String messageName = "Proto3Types";

        SourceTableFactory tableFactory = FactoryUtil.discoverConnectorFactory(SourceTableFactory.class, "inline");
        Map<String, String> options = new HashMap<>();
        options.put("repeat.count", "3");
        options.put("data", Base64.getEncoder().encodeToString(inputDatas.msg.toByteArray()));
        options.put("type", "base64");
        options.put("format", "protobuf");
        options.put("protobuf.descriptor.file.path", path);
        options.put("protobuf.message.name", messageName);

        Configuration configuration = Configuration.fromMap(options);
        ConnectorFactory.Context context = new ConnectorFactory.Context( null, options, configuration);
        SourceProvider sourceProvider = tableFactory.getSourceProvider(context);

        SinkTableFactory sinkTableFactory = FactoryUtil.discoverConnectorFactory(SinkTableFactory.class, "print");
        options = new HashMap<>();
        options.put("format", "json");
        configuration = Configuration.fromMap(options);
        context = new ConnectorFactory.Context( null, options, configuration);
        SinkProvider sinkProvider = sinkTableFactory.getSinkProvider(context);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = sourceProvider.produceDataStream(env);

        DataStreamSink<?> dataStreamSink = sinkProvider.consumeDataStream(dataStream);
        dataStreamSink.uid("sink").setParallelism(1);

        System.out.println(JSON.toJSONString(inputDatas.map));
        env.execute("test");
    }



}