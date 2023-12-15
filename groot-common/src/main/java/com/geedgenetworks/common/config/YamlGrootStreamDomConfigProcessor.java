package com.geedgenetworks.common.config;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.AbstractDomConfigProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.config.DomConfigHelper.*;

public class YamlGrootStreamDomConfigProcessor extends AbstractDomConfigProcessor {
    private static final ILogger LOGGER = Logger.getLogger(YamlGrootStreamDomConfigProcessor.class);

    private final GrootStreamConfig config;

    YamlGrootStreamDomConfigProcessor(boolean domLevel3, GrootStreamConfig config) {
        super(domLevel3);
        this.config = config;
    }

/*    @Override
    public void buildConfig(Node rootNode) {
        for (Node node : childElements(rootNode)) {
            String nodeName = cleanNodeName(node);
            if (occurrenceSet.contains(nodeName)) {
                throw new InvalidConfigurationException(
                        "Duplicate '" + nodeName + "' definition found in the configuration.");
            }
            if (handleNode(node, nodeName)) {
                continue;
            }
            if (!GrootStreamConfigSections.canOccurMultipleTimes(nodeName)) {
                occurrenceSet.add(nodeName);
            }
        }
    }*/

/*    private boolean handleNode(Node node, String name) {
        if (GrootStreamConfigSections.GROOTSTREAM.isEqual(name)) {
            parseEngineConfig(node, config);
        } else {
            return true;
        }
        parseEngineConfig(node, config);

        return false;
    }*/

    public void buildConfig(Node engineNode) {
        final EngineConfig engineConfig = config.getEngineConfig();
        for (Node node : childElements(engineNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.HTTP_CON_POOL.key().equals(name)) {
                engineConfig.setHttpConPoolConfig(parseHttpConPoolConfig(node));
            } else if (ServerConfigOptions. KNOWLEDGE_BASE.key().equals(name)) {
                engineConfig.setKnowledgeBaseConfig(parseKnowledgeBaseConfig(node));
            } else if (ServerConfigOptions.NACOS.key().equals(name)) {
                engineConfig.setNacosConfig(parseNacosConfig(node));
            } else if (ServerConfigOptions.CONSUL.key().equals(name)) {
                engineConfig.setConsulConfig(parseConsulConfig(node));
            } else if (ServerConfigOptions.ZOOKEEPER.key().equals(name)) {
                engineConfig.setZookeeperConfig(parseZookeeperConfig(node));
            } else if (ServerConfigOptions.HDFS.key().equals(name)) {
                engineConfig.setHdfsConfig(parseHdfsConfig(node));
            } else if (ServerConfigOptions.PROPERTIES.key().equals(name)) {
                engineConfig.setPropertiesConfig(parsePropertiesConfig(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);

            }

        }
    }

    private Map<String, String> parsePropertiesConfig(Node properties) {

            Map<String, String> propertiesMap = new HashMap<>();
            for (Node node : childElements(properties)) {
                String name = cleanNodeName(node);
                propertiesMap.put(name,getTextContent(node));
            }
            return propertiesMap;
    }


    private ZookeeperConfig parseZookeeperConfig(Node zookeeperNode) {
        ZookeeperConfig zookeeperConfig = new ZookeeperConfig();
        for (Node node : childElements(zookeeperNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.ZOOKEEPER_QUORUM.key().equals(name)) {
                zookeeperConfig.setQuorum(getTextContent(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return zookeeperConfig;
    }

    private HdfsConfig parseHdfsConfig(Node hdfsNode) {
        HdfsConfig hdfsConfig = new HdfsConfig();
        for (Node node : childElements(hdfsNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.HDFS_SERVERS.key().equals(name)) {
                hdfsConfig.setServers(getTextContent(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return hdfsConfig;
    }

    private ConsulConfig parseConsulConfig(Node consulNode) {
        ConsulConfig consulConfig = new ConsulConfig();
        for (Node node : childElements(consulNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.CONSUL_SERVER_ADDR.key().equals(name)) {
                consulConfig.setServerAddr(getTextContent(node));
            } else if (ServerConfigOptions.CONSUL_SERVER_PORT.key().equals(name)) {
                consulConfig.setServerPort(getIntegerValue(ServerConfigOptions.CONSUL_SERVER_PORT.key(), getTextContent(node)));
            } else if (ServerConfigOptions.CONSUL_TOKEN.key().equals(name)) {
                consulConfig.setToken(getTextContent(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return consulConfig;
    }

    private NacosConfig parseNacosConfig(Node nacosNode) {
        NacosConfig nacosConfig = new NacosConfig();
        for (Node node : childElements(nacosNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.NACOS_SERVER_ADDR.key().equals(name)) {
                nacosConfig.setServerAddr(getTextContent(node));
            } else if (ServerConfigOptions.NACOS_NAMESPACE.key().equals(name)) {
                nacosConfig.setNamespace(getTextContent(node));
            } else if (ServerConfigOptions.NACOS_GROUP.key().equals(name)) {
                nacosConfig.setGroup(getTextContent(node));
            } else if (ServerConfigOptions.NACOS_DATA_ID.key().equals(name)) {
                nacosConfig.setDataId(getTextContent(node));
            } else if (ServerConfigOptions.NACOS_READ_TIMEOUT.key().equals(name)) {
                nacosConfig.setReadTimeout(getIntegerValue(ServerConfigOptions.NACOS_READ_TIMEOUT.key(), getTextContent(node)));
            } else if (ServerConfigOptions.NACOS_USERNAME.key().equals(name)) {
                nacosConfig.setUserName(getTextContent(node));
            } else if (ServerConfigOptions.NACOS_PASSWORD.key().equals(name)) {
                nacosConfig.setPassword(getTextContent(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return nacosConfig;
    }


    private HttpConPoolConfig parseHttpConPoolConfig(Node httpConPoolNode) {
        HttpConPoolConfig httpConPoolConfig = new HttpConPoolConfig();
        for (Node node : childElements(httpConPoolNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.HTTP_CON_POOL_MAX_TOTAL.key().equals(name)) {
                httpConPoolConfig.setMaxTotal(getIntegerValue(ServerConfigOptions.HTTP_CON_POOL_MAX_TOTAL.key(), getTextContent(node)));
            } else if (ServerConfigOptions.HTTP_CON_POOL_MAX_PER_ROUTE.key().equals(name)) {
                httpConPoolConfig.setMaxPerRoute(getIntegerValue(ServerConfigOptions.HTTP_CON_POOL_MAX_PER_ROUTE.key(), getTextContent(node)));
            } else if (ServerConfigOptions.HTTP_CON_POOL_CONNECTION_REQUEST_TIMEOUT.key().equals(name)) {
                httpConPoolConfig.setConnectionRequestTimeout(getIntegerValue(ServerConfigOptions.HTTP_CON_POOL_CONNECTION_REQUEST_TIMEOUT.key(), getTextContent(node)));
            } else if (ServerConfigOptions.HTTP_CON_POOL_CONNECT_TIMEOUT.key().equals(name)) {
                httpConPoolConfig.setConnectTimeout(getIntegerValue(ServerConfigOptions.HTTP_CON_POOL_CONNECT_TIMEOUT.key(), getTextContent(node)));
            } else if (ServerConfigOptions.HTTP_CON_POOL_SOCKET_TIMEOUT.key().equals(name)) {
                httpConPoolConfig.setSocketTimeout(getIntegerValue(ServerConfigOptions.HTTP_CON_POOL_SOCKET_TIMEOUT.key(), getTextContent(node)));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return httpConPoolConfig;

    }

    private List<KnowledgeConfig> parseKnowledgeBaseConfig(Node knowledgeBaseNode) {

        List<KnowledgeConfig> knowledgeConfigList = new ArrayList<>();
        for (Node node : childElements(knowledgeBaseNode)) {
            knowledgeConfigList.add(parseKnowledgeConfig(node));
        }
        return knowledgeConfigList;
    }

   private KnowledgeConfig parseKnowledgeConfig(Node asnLookupNode) {
        KnowledgeConfig knowledgeConfig = new KnowledgeConfig();
        for (Node node : childElements(asnLookupNode)) {
            String name = cleanNodeName(node);

            if (ServerConfigOptions.KNOWLEDGE_BASE_NAME.key().equals(name)) {
                knowledgeConfig.setName(getTextContent(node));
            } else if (ServerConfigOptions.KNOWLEDGE_BASE_TYPE.key().equals(name)) {
                knowledgeConfig.setType(getTextContent(node));
            } else if (ServerConfigOptions.KNOWLEDGE_BASE_FILES.key().equals(name)) {
                knowledgeConfig.setFiles(parseFilesConfig(node));
            } else if (ServerConfigOptions.KNOWLEDGE_BASE_PROPERTIES.key().equals(name)) {
                knowledgeConfig.setProperties(parseKnowledgePropertiesConfig(node));
            }
            else{
                    LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return knowledgeConfig;
    }

    private Map<String, String> parseKnowledgePropertiesConfig(Node properties) {
        Map<String, String> propertiesMap = new HashMap<>();
        for (Node node : childElements(properties)) {
            String name = cleanNodeName(node);
            propertiesMap.put(name,getTextContent(node));
        }
        return propertiesMap;
    }

    private List<String> parseFilesConfig(Node files) {

        List<String> asnLookupConfigList = new ArrayList<>();
        for (Node node : childElements(files)) {
            String value = node.getNodeValue();
            asnLookupConfigList.add(value);
        }
        return asnLookupConfigList;
    }


}
