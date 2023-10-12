package com.geedgenetworks.connectors.ipfix.source;

import cn.hutool.core.io.IoUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.SourceConfig;
import com.geedgenetworks.source.Source;
import com.geedgenetworks.connectors.ipfix.util.IPFIXUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.cert.netsa.io.ipfix.*;
import scala.collection.Iterator;
import scala.collection.concurrent.TrieMap;
import scala.xml.Node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.geedgenetworks.config.commonConfig.*;
import static com.geedgenetworks.utils.ColumnUtil.columnSelector;

public class IPFIXSource implements Source {
    private static final Log logger = LogFactory.get();

    private static SessionGroup sessionGroup;

    static {
        InfoModel model = InfoModel.getStandardInfoModel(true);
        sessionGroup = new SessionGroup(model, null, false);
        ArrayList<Node> elements = IPFIXUtils.getElements();
        for (Node element : elements) {
            model.importElements(element);
        }
    }

    @Override
    public SingleOutputStreamOperator<Event> source(StreamExecutionEnvironment env, SourceConfig sourceConfig) {
        Map<String, Object> ipfixConfig = sourceConfig.getProperties();
        UDPSource udpSource = new UDPSource(Integer.parseInt(ipfixConfig.get("initial_port").toString()), Integer.parseInt(ipfixConfig.get("max_attempts").toString()), Integer.parseInt(ipfixConfig.get("buffer_size").toString()), Integer.parseInt(ipfixConfig.get("receive_buffer").toString()), ipfixConfig.get("service_name").toString());
        DataStream<byte[]> sourceForSession = env.addSource(udpSource).setParallelism(Integer.parseInt(sourceConfig.getProperties().getOrDefault("port_number", 1).toString()));
        SingleOutputStreamOperator<Event> streamEvent = sourceForSession.flatMap(new FlatMapFunction<byte[], Event>() {
            @Override
            public void flatMap(byte[] data, Collector<Event> out) {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    RecordReader reader = new RecordReader(new ByteBufferMessageReader(buffer, sessionGroup));
                    if (reader.isEmpty()) {
                        Iterator<Session> iterator = sessionGroup.iterator();
                        while (iterator.hasNext()) {
                            Session next = iterator.next();
                            logger.info("domain: " + next.observationDomain());
                            TrieMap<Object, Template> objectTemplateTrieMap = next.idGetTmpl();
                            logger.info("Template: " + objectTemplateTrieMap.toString());
                        }
                    } else {
                        while (reader.hasNext()) {
                            Event grootevent = new Event();
                            Record record = reader.next();
                            Iterator<Field> fields = record.fields();
                            Map<String, Object> fieldMap = new HashMap<>();
                            while (fields.hasNext()) {
                                Field field = fields.next();
                                fieldMap.put(field.ie().name(), field.value());
                            }
                            long epochSecond = record.exportTime().get().getEpochSecond();
                            grootevent.set__timestamp(epochSecond);
                            if (sourceConfig.getProperties().containsKey("message_timestamp_field")) {

                                fieldMap.put(sourceConfig.getProperties().get("message_timestamp_field").toString(), epochSecond);
                            }
                            if (sourceConfig.getOutput_fields() != null && sourceConfig.getOutput_fields().size() > 0) {
                                fieldMap = columnSelector(fieldMap, sourceConfig.getOutput_fields());
                            }
                            grootevent.setExtractedFields(fieldMap);
                            grootevent.setFinal(true);
                            out.collect(grootevent);
                        }
                    }
                } catch (Exception e) {
                    logger.error("IPFIXSource get record error.", e);
                }
            }
        });
        if (sourceConfig.getParallelism() != 0) {
            streamEvent.setParallelism(sourceConfig.getParallelism());
        }
        return streamEvent;
    }
}

class UDPSource implements ParallelSourceFunction<byte[]> {
    private static final Log logger = LogFactory.get();

    private final int initial_port;
    private final int max_attempts;
    private final int buffer_size;
    private final int receive_buffer;
    private final String service_name;
    private transient DatagramChannel channel;
    private transient NamingService namingService;
    private int currentPort;
    private String serverIP;

    public UDPSource(int initial_port, int max_attempts, int buffer_size, int receive_buffer, String service_name) {
        this.initial_port = initial_port;
        this.buffer_size = buffer_size;
        this.max_attempts = max_attempts;
        this.receive_buffer = receive_buffer;
        this.service_name = service_name;
    }

    @Override
    public void run(SourceContext<byte[]> ctx) {
        try {
            channel = DatagramChannel.open();
            channel.setOption(StandardSocketOptions.SO_RCVBUF, receive_buffer);
            channel.configureBlocking(false);
            currentPort = initial_port;
            int attempt = 1;
            while (attempt <= max_attempts) {
                try {
                    channel.bind(new InetSocketAddress(currentPort));
                    Properties properties = new Properties();
                    properties.setProperty(PropertyKeyConst.SERVER_ADDR, NACOS_SERVER_ADDR);
                    properties.put("username", NACOS_USERNAME);
                    properties.put("password", NACOS_PASSWORD);
                    InetAddress inetAddress = InetAddress.getLocalHost();
                    serverIP = inetAddress.getHostAddress();
                    namingService = NamingFactory.createNamingService(properties);
                    namingService.registerInstance(service_name, serverIP, currentPort);
                    break;
                } catch (IOException e) {
                    // 捕获端口占用异常
                    if (e.getMessage().contains("Address already in use")) {
                        logger.info("端口 " + currentPort + " 已被占用");
                        currentPort++;
                        attempt++;
                    } else {
                        logger.error(e.toString());
                        channel = null;
                        break;
                    }

                } catch (NacosException e) {
                    logger.error(e.toString());
                    channel = null;
                }

            }
            ByteBuffer buffer = ByteBuffer.allocate(buffer_size);
            while (true) {
                if (channel.receive(buffer) != null) {
                    buffer.flip();
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);
                    ctx.collect(data);
                    buffer.clear();
                }
            }
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
    }

    @Override
    public void cancel() {
        IoUtil.close(channel);
        try {
            namingService.deregisterInstance(service_name, serverIP, currentPort);
        } catch (NacosException e) {
            logger.error(e.toString(), e);
        }

    }


    public DatagramChannel findAvailablePort(DatagramChannel channel, int initialPort, int maxAttempts, String service_name) {
        int currentPort = initialPort;
        int attempt = 1;
        while (attempt <= maxAttempts) {
            try {
                channel.bind(new InetSocketAddress(currentPort));
                Properties properties = new Properties();
                properties.setProperty(PropertyKeyConst.SERVER_ADDR, NACOS_SERVER_ADDR);
                properties.put("username", NACOS_USERNAME);
                properties.put("password", NACOS_PASSWORD);
                InetAddress inetAddress = InetAddress.getLocalHost();
                String serverIP = inetAddress.getHostAddress();
                NamingService namingService = NamingFactory.createNamingService(properties);
                namingService.registerInstance(service_name, serverIP, currentPort);
                return channel; // 找到可用端口
            } catch (IOException e) {
                // 捕获端口占用异常
                if (e.getMessage().contains("Address already in use")) {
                    logger.info("端口 " + currentPort + " 已被占用");
                    currentPort++;
                    attempt++;
                } else {
                    logger.error(e.toString());
                    break;
                }
            } catch (NacosException e) {
                logger.error(e.toString());
            }
        }
        return null; // 无法找到可用端口
    }
}
