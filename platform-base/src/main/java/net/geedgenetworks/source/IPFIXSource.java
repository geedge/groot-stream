package net.geedgenetworks.source;

import cn.hutool.core.io.IoUtil;
import net.geedgenetworks.pojo.GrootEvent;
import net.geedgenetworks.pojo.SourceConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.cert.netsa.io.ipfix.*;
import scala.collection.Iterator;

import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class IPFIXSource implements Source {
    private static InfoModel model = InfoModel.getStandardInfoModel(true);
    private static SessionGroup sessionGroup = new SessionGroup(model, null, false);

    @Override
    public SingleOutputStreamOperator<GrootEvent> source(StreamExecutionEnvironment env, SourceConfig sourceConfig){
        Map<String, Object> ipfixConfig = sourceConfig.getProperties();
        UDPSource udpSource = new UDPSource(Integer.parseInt(ipfixConfig.get("port").toString()), Integer.parseInt(ipfixConfig.get("buffer_size").toString()));
        DataStream<byte[]> sourceForSession = env.addSource(udpSource).setParallelism(sourceConfig.getParallelism());
        WatermarkStrategy<GrootEvent> strategyForSession = WatermarkStrategy
                .<GrootEvent>forBoundedOutOfOrderness(Duration.ofMillis(sourceConfig.getWatermark_lag()))
                .withTimestampAssigner((StreamlineEvent, timestamp) -> System.currentTimeMillis());
        SingleOutputStreamOperator<GrootEvent> streamEvent = sourceForSession.map(new MapFunction<byte[], GrootEvent>() {
            @Override
            public GrootEvent map(byte[] data) {
                GrootEvent grootevent = new GrootEvent();
                grootevent.setFlag(true);
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    RecordReader reader = new RecordReader(new ByteBufferMessageReader(buffer, sessionGroup));
                    while (reader.hasNext()) {
                        Record record = reader.next();
                        Iterator<Field> fields = record.fields();
                        Map<String, Object> fieldMap = new HashMap<>();
                        while (fields.hasNext()) {
                            Field field = fields.next();
                            fieldMap.put(field.ie().name(), field.value());
                        }
                        System.out.println(fieldMap);
                        grootevent.setAuxiliaryFieldsAndValues(fieldMap);
                    }
                    return grootevent;
                } catch (Exception e) {
                    grootevent.setFlag(false);
                    return grootevent;
                }
            }
        }).setParallelism(sourceConfig.getParallelism()).assignTimestampsAndWatermarks(strategyForSession);
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
