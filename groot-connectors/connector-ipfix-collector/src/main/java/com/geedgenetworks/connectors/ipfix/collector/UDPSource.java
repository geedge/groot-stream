package com.geedgenetworks.connectors.ipfix.collector;

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
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class UDPSource extends RichParallelSourceFunction<byte[]> {
    private static final Log logger = LogFactory.get();

    private static ScheduledExecutorService executorService;
    private final int initialPort;
    private final int maxAttempts;
    private final int bufferSize;
    private final int receiveBuffer;
    private final String serviceName;
    private int currentPort;
    private String serverIp;
    private final int registryMode;
    private final Integer healthCheckInterval;
    private final String nacosServerAddr;
    private final String nacosUsername;
    private final String nacosPassword;
    private final String nacosNamespace;
    private final String nacosGroup;
    private final String consulServerAddr;
    private final Integer consulServerPort;
    private final String consulToken;
    private transient DatagramChannel channel;
    private transient NamingService namingService;
    private transient ConsulClient consulClient;
    private transient boolean stop;

    public UDPSource(int initialPort, int maxAttempts, int bufferSize, int receiveBuffer, Properties serviceDiscoveryProperties) {
        this.initialPort = initialPort;
        this.bufferSize = bufferSize;
        this.maxAttempts = maxAttempts;
        this.receiveBuffer = receiveBuffer;
        this.registryMode = Integer.parseInt(String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_REGISTRY_MODE.key())));
        this.serviceName = registryMode == 0 || registryMode == 1 ? String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_SERVICE_NAME.key())) : null;
        this.healthCheckInterval = registryMode == 0 || registryMode == 1 ? Integer.parseInt(String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_HEALTH_CHECK_INTERVAL.key()))) : null;
        this.nacosServerAddr = registryMode == 0 ? String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_SERVER_ADDR.key())) : null;
        this.nacosUsername = registryMode == 0 ? String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_USERNAME.key())) : null;
        this.nacosPassword = registryMode == 0 ? String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_PASSWORD.key())) : null;
        this.nacosNamespace = registryMode == 0 ? String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_NAMESPACE.key())) : null;
        this.nacosGroup = registryMode == 0 ? String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_GROUP.key())) : null;
        this.consulServerAddr = registryMode == 1 ? String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_CONSUL_SERVER_ADDR.key())) : null;
        this.consulServerPort = registryMode == 1 ? Integer.parseInt(String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_CONSUL_SERVER_PORT.key()))) : null;
        this.consulToken = registryMode == 1 ? String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_CONSUL_TOKEN.key())) : null;
    }

    @Override
    public void run(SourceContext<byte[]> ctx) {
        try {
            executorService = Executors.newScheduledThreadPool(maxAttempts);
            channel = DatagramChannel.open();
            channel.setOption(StandardSocketOptions.SO_RCVBUF, receiveBuffer);
            channel.configureBlocking(false);
            Selector selector = Selector.open();
            currentPort = initialPort;
            int attempt = 1;
            while (attempt <= maxAttempts) {
                try {
                    channel.bind(new InetSocketAddress(currentPort));
                    channel.register(selector, SelectionKey.OP_READ);
                    serverIp = InetAddress.getLocalHost().getHostAddress();
                    registerInstance(serverIp, currentPort);
                    break;
                } catch (IOException e) {
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
            while (!stop) {
                selector.select();
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey selectionKey = keyIterator.next();
                    if (selectionKey.isReadable()) {
                        DatagramChannel currentDatagramChannel = (DatagramChannel) selectionKey.channel();
                        currentDatagramChannel.receive(buffer);
                        buffer.flip();
                        byte[] data = new byte[buffer.remaining()];
                        buffer.get(data);
                        ctx.collect(data);
                        buffer.clear();
                    }
                    keyIterator.remove();
                }
            }
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
    }

    @Override
    public void cancel() {
        stop = true;
        executorService.shutdownNow();
        IoUtil.close(channel);
        deregisterInstance(serverIp, currentPort);
    }

    private void registerInstance(String ip, int port) throws NacosException {
        if (registryMode == 0) {
            Properties properties = new Properties();
            properties.put(PropertyKeyConst.SERVER_ADDR, nacosServerAddr);
            properties.put(PropertyKeyConst.USERNAME, nacosUsername);
            properties.put(PropertyKeyConst.PASSWORD, nacosPassword);
            properties.put(PropertyKeyConst.NAMESPACE, nacosNamespace);
            namingService = NamingFactory.createNamingService(properties);
            Instance instance = new Instance();
            instance.setIp(ip);
            instance.setPort(port);
            instance.setEphemeral(true);
            namingService.registerInstance(serviceName, nacosGroup, instance);
            executorService.scheduleAtFixedRate(() -> {
                try {
                    namingService.registerInstance(serviceName, nacosGroup, instance);
                } catch (NacosException e) {
                    logger.error("nacos check instance healthy error.", e);
                }
            }, 0, healthCheckInterval, TimeUnit.SECONDS);
        } else if (registryMode == 1) {
            consulClient = new ConsulClient(consulServerAddr, consulServerPort);
            NewService newService = new NewService();
            newService.setName(serviceName);
            newService.setId(ip + ":" + port);
            newService.setAddress(ip);
            newService.setPort(port);
            consulClient.agentServiceRegister(newService, consulToken);
            NewCheck newCheck = new NewCheck();
            newCheck.setTtl(healthCheckInterval + "s");
            newCheck.setName(ip + ":" + port);
            newCheck.setId(ip + ":" + port);
            newCheck.setServiceId(ip + ":" + port);
            newCheck.setDeregisterCriticalServiceAfter("60s");
            consulClient.agentCheckRegister(newCheck);
            executorService.scheduleAtFixedRate(() -> consulClient.agentCheckPass(ip + ":" + port, null, consulToken),
                    0, healthCheckInterval - 1, TimeUnit.SECONDS);
        }
    }

    private void deregisterInstance(String ip, int port) {
        try {
            if (registryMode == 0) {
                Instance instance = new Instance();
                instance.setIp(ip);
                instance.setPort(port);
                instance.setEphemeral(true);
                namingService.deregisterInstance(serviceName, nacosGroup, instance);
            } else if (registryMode == 1) {
                consulClient.agentServiceDeregister(ip + ":" + port, consulToken);
            }
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
    }
}
