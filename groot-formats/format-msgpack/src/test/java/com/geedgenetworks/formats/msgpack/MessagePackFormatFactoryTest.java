package com.geedgenetworks.formats.msgpack;

import com.geedgenetworks.spi.sink.SinkProvider;
import com.geedgenetworks.spi.sink.SinkTableFactory;
import com.geedgenetworks.spi.source.SourceProvider;
import com.geedgenetworks.spi.source.SourceTableFactory;
import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.factory.FactoryUtil;
import com.geedgenetworks.spi.table.factory.TableFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class MessagePackFormatFactoryTest {

    private static byte[] getTestBytes() throws Exception{
        ValueFactory.MapBuilder map = ValueFactory.newMapBuilder();
        map.put(ValueFactory.newString("uint8"), ValueFactory.newInteger(123));
        map.put(ValueFactory.newString("uint16"), ValueFactory.newInteger(512));
        map.put(ValueFactory.newString("uint32"), ValueFactory.newInteger(33554432));
        map.put(ValueFactory.newString("uint64"), ValueFactory.newInteger(17179869184L));
        map.put(ValueFactory.newString("int8"), ValueFactory.newInteger(-123));
        map.put(ValueFactory.newString("int16"), ValueFactory.newInteger(-512));
        map.put(ValueFactory.newString("int32"), ValueFactory.newInteger(-33554432));
        map.put(ValueFactory.newString("int64"), ValueFactory.newInteger(-17179869184L));
        map.put(ValueFactory.newString("null"), ValueFactory.newNil());

        map.put(ValueFactory.newString("double"), ValueFactory.newFloat(123.2));

        map.put(ValueFactory.newString("bool_true"), ValueFactory.newBoolean(true));
        map.put(ValueFactory.newString("bool_false"), ValueFactory.newBoolean(false));

        map.put(ValueFactory.newString("str"), ValueFactory.newString("ut8字符串"));

        map.put(ValueFactory.newString("binary"), ValueFactory.newBinary("ut8字符串".getBytes(StandardCharsets.UTF_8)));

        map.put(ValueFactory.newString("int32_array"), ValueFactory.newArray(ValueFactory.newInteger(123), ValueFactory.newInteger(512), ValueFactory.newNil(), ValueFactory.newInteger(33554432)));
        map.put(ValueFactory.newString("str_array"), ValueFactory.newArray(ValueFactory.newString("ut8字符串1"), ValueFactory.newNil(), ValueFactory.newString("ut8字符串2")));

        map.put(ValueFactory.newString("obj"), ValueFactory.newMapBuilder()
                .put(ValueFactory.newString("uint8"), ValueFactory.newInteger(123))
                .put(ValueFactory.newString("uint32"), ValueFactory.newInteger(33554432))
                .put(ValueFactory.newString("double"), ValueFactory.newFloat(123.2))
                .put(ValueFactory.newString("str"), ValueFactory.newString("ut8字符串"))
                .build());


        MapValue mapValue = map.build();
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packValue(mapValue);
        byte[] bytes = packer.toByteArray();
        packer.close();
        return bytes;
    }

    public static void main(String[] args) throws Exception{
        byte[] bytes = getTestBytes();

        SourceTableFactory tableFactory = FactoryUtil.discoverTableFactory(SourceTableFactory.class, "inline");
        Map<String, String> options = new HashMap<>();
        options.put("data", Base64.getEncoder().encodeToString(bytes));
        options.put("repeat.count", "3");
        options.put("type", "base64");
        options.put("format", "msgpack");

        Configuration configuration = Configuration.fromMap(options);
        TableFactory.Context context = new TableFactory.Context( null, options, configuration);
        SourceProvider sourceProvider = tableFactory.getSourceProvider(context);


        SinkTableFactory sinkTableFactory = FactoryUtil.discoverTableFactory(SinkTableFactory.class, "print");
        options = new HashMap<>();
        options.put("format", "msgpack");
        configuration = Configuration.fromMap(options);
        context = new TableFactory.Context( null, options, configuration);
        SinkProvider sinkProvider = sinkTableFactory.getSinkProvider(context);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = sourceProvider.produceDataStream(env);

        DataStreamSink<?> dataStreamSink = sinkProvider.consumeDataStream(dataStream);
        dataStreamSink.uid("sink").setParallelism(1);

        env.execute("test");
    }



}
