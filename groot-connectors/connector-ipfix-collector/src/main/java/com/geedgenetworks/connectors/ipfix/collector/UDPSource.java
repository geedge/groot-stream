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
import java.net.*;
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
    private static final Log LOG = LogFactory.get();

    private final int initialPort;
    private final int bufferSize;
    private final int receiveBuffer;
    private final Properties serviceDiscoveryProperties;
    private int currentPort;
    private String serverIp;
    private DatagramChannel channel;
    private NamingService namingService;
    private ConsulClient consulClient;
    private volatile boolean stop;
    private ScheduledExecutorService executorService;

    public UDPSource(int initialPort, int bufferSize, int receiveBuffer, Properties serviceDiscoveryProperties) {
        this.initialPort = initialPort;
        this.bufferSize = bufferSize;
        this.receiveBuffer = receiveBuffer;
        this.serviceDiscoveryProperties = serviceDiscoveryProperties;
    }

    @Override
    public void run(SourceContext<byte[]> ctx) throws IOException, NacosException {
        executorService = Executors.newScheduledThreadPool(1);
        channel = DatagramChannel.open();
        channel.setOption(StandardSocketOptions.SO_RCVBUF, receiveBuffer);
        channel.configureBlocking(false);
        Selector selector = Selector.open();
        currentPort = initialPort;
        boolean portAvailable = false;
        while (!portAvailable) {
            try {
                channel.bind(new InetSocketAddress(currentPort));
                portAvailable = true;
            } catch (BindException e) {
                currentPort++;
            }
        }
        channel.register(selector, SelectionKey.OP_READ);
        serverIp = InetAddress.getLocalHost().getHostAddress();
        registerInstance(serverIp, currentPort);
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
    }

    @Override
    public void cancel() {
        stop = true;
        executorService.shutdownNow();
        IoUtil.close(channel);
        deregisterInstance(serverIp, currentPort);
    }

    private void registerInstance(String ip, int port) throws NacosException {
        int registryMode = Integer.parseInt(String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_REGISTRY_MODE.key())));
        String serviceName = String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_SERVICE_NAME.key()));
        int healthCheckInterval = Integer.parseInt(String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_HEALTH_CHECK_INTERVAL.key())));
        if (registryMode == 0) {
            String nacosGroup = String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_GROUP.key()));
            Properties properties = new Properties();
            properties.put(PropertyKeyConst.SERVER_ADDR, String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_SERVER_ADDR.key())));
            properties.put(PropertyKeyConst.USERNAME, String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_USERNAME.key())));
            properties.put(PropertyKeyConst.PASSWORD, String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_PASSWORD.key())));
            properties.put(PropertyKeyConst.NAMESPACE, String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_NAMESPACE.key())));
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
                    LOG.error("nacos check instance healthy error.", e);
                }
            }, 0, healthCheckInterval, TimeUnit.SECONDS);
        } else if (registryMode == 1) {
            String consulToken = String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_CONSUL_TOKEN.key()));
            consulClient = new ConsulClient(String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_CONSUL_SERVER_ADDR.key())), Integer.parseInt(String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_CONSUL_SERVER_PORT.key()))));
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
            int registryMode = Integer.parseInt(String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_REGISTRY_MODE.key())));
            if (registryMode == 0) {
                Instance instance = new Instance();
                instance.setIp(ip);
                instance.setPort(port);
                instance.setEphemeral(true);
                namingService.deregisterInstance(String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_SERVICE_NAME.key())), String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_NACOS_GROUP.key())), instance);
            } else if (registryMode == 1) {
                consulClient.agentServiceDeregister(ip + ":" + port, String.valueOf(serviceDiscoveryProperties.get(IPFixConnectorOptions.SERVICE_DISCOVERY_CONSUL_TOKEN.key())));
            }
        } catch (NacosException e) {
            LOG.error(e.toString(), e);
        }
    }

}