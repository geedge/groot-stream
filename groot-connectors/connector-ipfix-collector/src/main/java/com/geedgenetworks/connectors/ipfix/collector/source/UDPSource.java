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
import com.geedgenetworks.common.config.ConsulConfig;
import com.geedgenetworks.common.config.NacosConfig;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class UDPSource extends RichParallelSourceFunction<byte[]> {
    private static final Log logger = LogFactory.get();

    private final int initialPort;
    private final int maxAttempts;
    private final int bufferSize;
    private final int receiveBuffer;
    private final String serviceName;
    private transient DatagramChannel channel;
    private transient NamingService namingService;
    private transient ConsulClient consulClient;
    private int currentPort;
    private String serverIp;
    private int registerMode;
    private int healthCheckInterval;
    private NacosConfig nacosConfig;
    private ConsulConfig consulConfig;

    public UDPSource(int initialPort, int maxAttempts, int bufferSize, int receiveBuffer, String serviceName, int registerMode, int healthCheckInterval, NacosConfig nacosConfig, ConsulConfig consulConfig) {
        this.initialPort = initialPort;
        this.bufferSize = bufferSize;
        this.maxAttempts = maxAttempts;
        this.receiveBuffer = receiveBuffer;
        this.serviceName = serviceName;
        this.registerMode = registerMode;
        this.healthCheckInterval = healthCheckInterval;
        this.nacosConfig = nacosConfig;
        this.consulConfig = consulConfig;
    }

    @Override
    public void run(SourceContext<byte[]> ctx) {
        try {
            channel = DatagramChannel.open();
            channel.setOption(StandardSocketOptions.SO_RCVBUF, receiveBuffer);
            channel.configureBlocking(false);
            currentPort = initialPort;
            int attempt = 1;
            while (attempt <= maxAttempts) {
                try {
                    channel.bind(new InetSocketAddress(currentPort));
                    InetAddress inetAddress = InetAddress.getLocalHost();
                    serverIp = inetAddress.getHostAddress();
                    registerInstance(serverIp, currentPort, nacosConfig, consulConfig);
                    break;
                } catch (IOException e) {
                    // 捕获端口占用异常
                    if (e.getMessage().contains("Address already in use")) {
                        logger.info("端口 " + currentPort + " 已被占用");
                        currentPort++;
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
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
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
        deregisterInstance(serverIp, currentPort, consulConfig);
    }

    private void registerInstance(String ip, int port, NacosConfig nacosConfig, ConsulConfig consulConfig) throws NacosException {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        if (registerMode == 0) {
            Properties properties = new Properties();
            properties.setProperty(PropertyKeyConst.SERVER_ADDR, nacosConfig.getServerAddr());
            properties.setProperty(PropertyKeyConst.USERNAME, nacosConfig.getUserName());
            properties.setProperty(PropertyKeyConst.PASSWORD, nacosConfig.getPassword());
            properties.setProperty(PropertyKeyConst.NAMESPACE, nacosConfig.getNamespace());
            namingService = NamingFactory.createNamingService(properties);
            Instance instance = new Instance();
            instance.setIp(ip);
            instance.setPort(port);
            instance.setEphemeral(true);
            namingService.registerInstance(serviceName, instance);
            executorService.scheduleAtFixedRate(() -> {
                try {
                    namingService.registerInstance(serviceName, instance);
                } catch (NacosException e) {
                    logger.error("nacos check instance healthy error.", e);
                }
            }, 0, healthCheckInterval, TimeUnit.SECONDS);
        } else if (registerMode == 1) {
            consulClient = new ConsulClient(consulConfig.getServerAddr(), consulConfig.getServerPort());
            NewService newService = new NewService();
            newService.setName(serviceName);
            newService.setId(ip + ":" + port);
            newService.setAddress(ip);
            newService.setPort(port);
            consulClient.agentServiceRegister(newService, consulConfig.getToken());
            NewCheck newCheck = new NewCheck();
            newCheck.setTtl(healthCheckInterval + "s");
            newCheck.setName(ip + ":" + port);
            newCheck.setId(ip + ":" + port);
            newCheck.setServiceId(ip + ":" + port);
            newCheck.setDeregisterCriticalServiceAfter("60s");
            consulClient.agentCheckRegister(newCheck);
            executorService.scheduleAtFixedRate(() -> consulClient.agentCheckPass(ip + ":" + port, null, consulConfig.getToken()),
                    0, healthCheckInterval - 1, TimeUnit.SECONDS);
        }
    }

    private void deregisterInstance(String ip, int port, ConsulConfig consulConfig) {
        try {
            if (registerMode == 0) {
                Instance instance = new Instance();
                instance.setIp(ip);
                instance.setPort(port);
                instance.setEphemeral(false);
                namingService.deregisterInstance(serviceName, instance);
            } else if (registerMode == 1) {
                consulClient.agentServiceDeregister(ip + ":" + port, consulConfig.getToken());
            }
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
    }
}
