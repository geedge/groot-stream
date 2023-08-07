package com.geedgenetworks.source;

import cn.hutool.core.io.IoUtil;
import com.geedgenetworks.pojo.SourceConfig;
import com.geedgenetworks.pojo.GrootEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.cert.netsa.io.ipfix.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.HashMap;
import java.util.Map;

public class IPFIXSource implements Source {
    private static final Logger logger = LoggerFactory.getLogger(IPFIXSource.class);

    private static InfoModel model = InfoModel.getStandardInfoModel(true);
    private static SessionGroup sessionGroup = new SessionGroup(model, null, false);

    @Override
    public SingleOutputStreamOperator<GrootEvent> source(StreamExecutionEnvironment env, SourceConfig sourceConfig) {
        Map<String, Object> ipfixConfig = sourceConfig.getProperties();
        UDPSource udpSource = new UDPSource(Integer.parseInt(ipfixConfig.get("port").toString()), Integer.parseInt(ipfixConfig.get("buffer_size").toString()));
        DataStream<byte[]> sourceForSession = env.addSource(udpSource).setParallelism(1);
        SingleOutputStreamOperator<GrootEvent> streamEvent = sourceForSession.flatMap(new FlatMapFunction<byte[], GrootEvent>() {
            @Override
            public void flatMap(byte[] data, Collector<GrootEvent> out) {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    RecordReader reader = new RecordReader(new ByteBufferMessageReader(buffer, sessionGroup));
                    GrootEvent grootevent = null;
                    while (reader.hasNext()) {
                        grootevent = new GrootEvent();
                        Record record = reader.next();
                        Iterator<Field> fields = record.fields();
                        Map<String, Object> fieldMap = new HashMap<>();
                        while (fields.hasNext()) {
                            Field field = fields.next();
                            fieldMap.put(field.ie().name(), field.value());
                        }
                        System.out.println(fieldMap);
                        grootevent.setAuxiliaryFieldsAndValues(fieldMap);
                        grootevent.setFlag(true);
                    }
                    out.collect(grootevent);
                } catch (Exception e) {
                    logger.error("IPFIXSource get record error." + e);
                }
            }
        }).setParallelism(sourceConfig.getParallelism());
        return streamEvent;
    }
}

class UDPSource implements SourceFunction<byte[]> {
    private final int port;
    private final int bufferSize;
    private transient DatagramChannel channel;

    public UDPSource(int port, int bufferSize) {
        this.port = port;
        this.bufferSize = bufferSize;
    }

    @Override
    public void run(SourceContext<byte[]> ctx) throws Exception {
        channel = DatagramChannel.open();
        channel.configureBlocking(false);
        channel.bind(new InetSocketAddress(port));
        while (true) {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                SocketAddress receive = channel.receive(buffer);
                if (receive != null) {
                    buffer.flip();
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);
                    ctx.collect(data);
                }
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }

    @Override
    public void cancel() {
        IoUtil.close(channel);
    }
}
