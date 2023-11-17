package com.geedgenetworks.connectors.ipfix.collector.source;

import cn.hutool.core.io.IoUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.agent.model.NewCheck;
import com.ecwid.consul.v1.agent.model.NewService;
import com.geedgenetworks.connectors.ipfix.collector.util.IPFIXUtils;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.SourceConfig;
import com.geedgenetworks.core.source.Source;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.geedgenetworks.common.config.commonConfig.*;
import static com.geedgenetworks.common.utils.ColumnUtil.columnSelector;

public class IPFixSource implements Source {
    private static final Log logger = LogFactory.get();

    private static SessionGroup session_group;

    static {
        InfoModel model = InfoModel.getStandardInfoModel(true);
        session_group = new SessionGroup(model, null, false);
        ArrayList<Node> elements = IPFIXUtils.getElements();
        for (Node element : elements) {
            model.importElements(element);
        }
    }

    @Override
    public SingleOutputStreamOperator<Event> source(StreamExecutionEnvironment env, SourceConfig sourceConfig) {
        Map<String, Object> ipFixConfig = sourceConfig.getProperties();
        UDPSource udpSource = new UDPSource(Integer.parseInt(ipFixConfig.get("initial_port").toString()),
                Integer.parseInt(ipFixConfig.get("max_attempts").toString()),
                Integer.parseInt(ipFixConfig.get("buffer_size").toString()),
                Integer.parseInt(ipFixConfig.get("receive_buffer").toString()),
                ipFixConfig.get("service_name").toString(),
                Integer.parseInt(ipFixConfig.get("register_mode").toString()),
                Integer.parseInt(ipFixConfig.get("health_check_interval").toString()));
        DataStream<byte[]> sourceForSession = env.addSource(udpSource).setParallelism(Integer.parseInt(sourceConfig.getProperties().getOrDefault("port_number", 1).toString()));
        SingleOutputStreamOperator<Event> streamEvent = sourceForSession.flatMap(new FlatMapFunction<byte[], Event>() {
            @Override
            public void flatMap(byte[] data, Collector<Event> out) {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    RecordReader reader = new RecordReader(new ByteBufferMessageReader(buffer, session_group));
                    if (reader.isEmpty()) {
                        Iterator<Session> iterator = session_group.iterator();
                        while (iterator.hasNext()) {
                            Session next = iterator.next();
                            logger.info("domain: " + next.observationDomain());
                            TrieMap<Object, Template> objectTemplateTrieMap = next.idGetTmpl();
                            logger.info("Template: " + objectTemplateTrieMap.toString());
                        }
                    } else {
                        while (reader.hasNext()) {
                            Event grootEvent = new Event();
                            Record record = reader.next();
                            Iterator<Field> fields = record.fields();
                            Map<String, Object> fieldMap = new HashMap<>();
                            while (fields.hasNext()) {
                                Field field = fields.next();
                                fieldMap.put(field.ie().name(), field.value());
                            }
                            long epochSecond = record.exportTime().get().getEpochSecond();
                            grootEvent.set__timestamp(epochSecond);
                            if (sourceConfig.getProperties().containsKey("message_timestamp_field")) {
                                fieldMap.put(sourceConfig.getProperties().get("message_timestamp_field").toString(), epochSecond);
                            }
                            if (sourceConfig.getOutput_fields() != null && sourceConfig.getOutput_fields().size() > 0) {
                                fieldMap = columnSelector(fieldMap, sourceConfig.getOutput_fields());
                            }
                            grootEvent.setExtractedFields(fieldMap);
                            grootEvent.setFinal(true);
                            out.collect(grootEvent);
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
    private transient NamingService naming_service;
    private transient ConsulClient consul_client;
    private int current_port;
    private String server_ip;
    private int register_mode;
    private int health_check_interval;

    public UDPSource(int initial_port, int max_attempts, int buffer_size, int receive_buffer, String service_name, int register_mode ,int health_check_interval) {
        this.initial_port = initial_port;
        this.buffer_size = buffer_size;
        this.max_attempts = max_attempts;
        this.receive_buffer = receive_buffer;
        this.service_name = service_name;
        this.register_mode = register_mode;
        this.health_check_interval = health_check_interval;
    }

    @Override
    public void run(SourceContext<byte[]> ctx) {
        try {
            channel = DatagramChannel.open();
            channel.setOption(StandardSocketOptions.SO_RCVBUF, receive_buffer);
            channel.configureBlocking(false);
            current_port = initial_port;
            int attempt = 1;
            while (attempt <= max_attempts) {
                try {
                    channel.bind(new InetSocketAddress(current_port));
                    InetAddress inetAddress = InetAddress.getLocalHost();
                    server_ip = inetAddress.getHostAddress();
                    registerInstance(server_ip, current_port);
                    break;
                } catch (IOException e) {
                    // 捕获端口占用异常
                    if (e.getMessage().contains("Address already in use")) {
                        logger.info("端口 " + current_port + " 已被占用");
                        current_port++;
                        attempt++;
                    } else {
                        logger.error(e.toString(), e);
                        channel = null;
                        break;
                    }
                } catch (NacosException e) {
                    logger.error(e.toString(), e);
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
        deregisterInstance(server_ip, current_port);
    }

    private void registerInstance(String ip, int port) throws NacosException {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        if (register_mode == 0) {
            Properties properties = new Properties();
            properties.setProperty(PropertyKeyConst.SERVER_ADDR, NACOS_SERVER_ADDR);
            properties.setProperty(PropertyKeyConst.USERNAME, NACOS_USERNAME);
            properties.setProperty(PropertyKeyConst.PASSWORD, NACOS_PASSWORD);
            properties.setProperty(PropertyKeyConst.NAMESPACE, NACOS_NAMESPACE);
            naming_service = NamingFactory.createNamingService(properties);
            Instance instance = new Instance();
            instance.setIp(ip);
            instance.setPort(port);
            instance.setEphemeral(true);
            naming_service.registerInstance(service_name, instance);
            executorService.scheduleAtFixedRate(() -> {
                try {
                    naming_service.registerInstance(service_name, instance);
                } catch (NacosException e) {
                    logger.error("nacos check instance healthy error.", e);
                }
            }, 0, health_check_interval, TimeUnit.SECONDS);
        } else {
            consul_client = new ConsulClient(CONSUL_SERVER_ADDR, CONSUL_SERVER_PORT);
            NewService newService = new NewService();
            newService.setName(service_name);
            newService.setId(ip + ":" + port);
            newService.setAddress(ip);
            newService.setPort(port);
            consul_client.agentServiceRegister(newService, CONSUL_TOKEN);
            NewCheck newCheck = new NewCheck();
            newCheck.setTtl(health_check_interval + "s");
            newCheck.setName(ip+":"+port);
            newCheck.setId(ip+":"+port);
            newCheck.setServiceId(ip+":"+port);
            newCheck.setDeregisterCriticalServiceAfter("60s");
            consul_client.agentCheckRegister(newCheck);
            executorService.scheduleAtFixedRate(() -> consul_client.agentCheckPass(ip+":"+port, null, CONSUL_TOKEN),
                    0, health_check_interval-1, TimeUnit.SECONDS);
        }
    }

    private void deregisterInstance(String ip, int port) {
        try {
            if (register_mode == 0) {
                Instance instance = new Instance();
                instance.setIp(ip);
                instance.setPort(port);
                instance.setEphemeral(false);
                naming_service.deregisterInstance(service_name, instance);
            } else {
                consul_client.agentServiceDeregister(ip + ":" + port, CONSUL_TOKEN);
            }
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
    }
}
