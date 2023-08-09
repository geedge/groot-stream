package com.geedgenetworks.source;

import cn.hutool.core.io.IoUtil;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.SourceConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.cert.netsa.io.ipfix.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.io.IOException;
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
    public SingleOutputStreamOperator<Event> source(StreamExecutionEnvironment env, SourceConfig sourceConfig) {
        Map<String, Object> ipfixConfig = sourceConfig.getProperties();
        UDPSource udpSource = new UDPSource(Integer.parseInt(ipfixConfig.get("initial_port").toString()),Integer.parseInt(ipfixConfig.get("max_attempts").toString()) ,Integer.parseInt(ipfixConfig.get("buffer_size").toString()));
        DataStream<byte[]> sourceForSession = env.addSource(udpSource).setParallelism(Integer.parseInt(sourceConfig.getProperties().getOrDefault("max_attempts",1).toString()));
        SingleOutputStreamOperator<Event> streamEvent = sourceForSession.flatMap(new FlatMapFunction<byte[], Event>() {
            @Override
            public void flatMap(byte[] data, Collector<Event> out) {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    RecordReader reader = new RecordReader(new ByteBufferMessageReader(buffer, sessionGroup));
                    Event grootevent = null;
                    while (reader.hasNext()) {
                        grootevent = new Event();
                        Record record = reader.next();
                        Iterator<Field> fields = record.fields();
                        Map<String, Object> fieldMap = new HashMap<>();
                        while (fields.hasNext()) {
                            Field field = fields.next();
                            fieldMap.put(field.ie().name(), field.value());
                        }
                        //System.out.println(fieldMap);
                        grootevent.setExtractedFields(fieldMap);
                        grootevent.setFinal(true);
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

class UDPSource implements ParallelSourceFunction<byte[]> {
    private final int initial_port;
    private final int max_attempts;
    private final int buffer_size;
    private transient DatagramChannel channel;
    private static final Logger logger = LoggerFactory.getLogger(UDPSource.class);

    public UDPSource(int initial_port,int max_attempts, int buffer_size) {
        this.initial_port = initial_port;
        this.buffer_size = buffer_size;
        this.max_attempts=max_attempts;
    }

    @Override
    public void run(SourceContext<byte[]> ctx) throws Exception {
        channel = DatagramChannel.open();
        channel.configureBlocking(false);

        channel=findAvailablePort(channel,initial_port,40);
        while (true) {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(buffer_size);
                SocketAddress receive = channel.receive(buffer);
                if (receive != null) {
                    buffer.flip();
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);
                    ctx.collect(data);
                }
            } catch (Exception e) {
                logger.error(e.toString());
            }
        }
    }
    public static DatagramChannel findAvailablePort(DatagramChannel channel,int initialPort, int maxAttempts) {
        int currentPort = initialPort;
        int attempt = 1;

        while (attempt <= maxAttempts) {
            try  {
                channel.bind(new InetSocketAddress(currentPort));
                return channel; // 找到可用端口
            } catch (IOException e) {
                // 捕获端口占用异常
                if (e.getMessage().contains("Address already in use")) {
                    System.out.println("端口 " + currentPort + " 已被占用");
                    currentPort++;
                    attempt++;
                } else {
                    logger.error(e.toString());
                    break;
                }
            }
        }

        return null; // 无法找到可用端口
    }
    @Override
    public void cancel() {
        IoUtil.close(channel);
    }
}
