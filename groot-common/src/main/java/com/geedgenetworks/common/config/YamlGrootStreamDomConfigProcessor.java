package com.geedgenetworks.common.config;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.AbstractDomConfigProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.config.DomConfigHelper.*;

public class YamlGrootStreamDomConfigProcessor extends AbstractDomConfigProcessor {
    private static final ILogger LOGGER = Logger.getLogger(YamlGrootStreamDomConfigProcessor.class);

    private final GrootStreamConfig config;

    YamlGrootStreamDomConfigProcessor(boolean domLevel3, GrootStreamConfig config) {
        super(domLevel3);
        this.config = config;
    }

    @Override
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
    }

    private boolean handleNode(Node node, String name) {
        if (GrootStreamConfigSections.ENGINE.isEqual(name)) {
            parseEngineConfig(node, config);
        } else {
            return true;
        }
        return false;
    }

    private void parseEngineConfig(Node engineNode, GrootStreamConfig config) {
        final EngineConfig engineConfig = config.getEngineConfig();
        for (Node node : childElements(engineNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.TICK_TUPLE_FREQ_SECS.key().equals(name)) {
                engineConfig.setTickTupleFreqSecs(getIntegerValue(ServerConfigOptions.TICK_TUPLE_FREQ_SECS.key(), getTextContent(node)));
            } else if (ServerConfigOptions.GTPC_SCAN_MAX_ROWS.key().equals(name)) {
                engineConfig.setGtpcScanMaxRows(getIntegerValue(ServerConfigOptions.GTPC_SCAN_MAX_ROWS.key(), getTextContent(node)));
            } else if (ServerConfigOptions.RADIUS_SCAN_MAX_ROWS.key().equals(name)) {
                engineConfig.setRadiusScanMaxRows(getIntegerValue(ServerConfigOptions.RADIUS_SCAN_MAX_ROWS.key(), getTextContent(node)));
            } else if (ServerConfigOptions.RADIUS_TABLE_NAME.key().equals(name)) {
                engineConfig.setRadiusTableName(getTextContent(node));
            } else if (ServerConfigOptions.GTPC_TABLE_NAME.key().equals(name)) {
                engineConfig.setGtpcTableName(getTextContent(node));
            } else if (ServerConfigOptions.HBASE_RPC_TIMEOUT.key().equals(name)) {
                engineConfig.setHbaseRpcTimeout(getIntegerValue(ServerConfigOptions.HBASE_RPC_TIMEOUT.key(), getTextContent(node)));
            } else if (ServerConfigOptions.HTTP_CON_POOL.key().equals(name)) {
                engineConfig.setHttpConPoolConfig(parseHttpConPoolConfig(node));
            } else if (ServerConfigOptions.KNOWLEDGE_BASE.key().equals(name)) {
                engineConfig.setKnowledgeBaseConfig(parseKnowledgeBaseConfig(node));
            } else if (ServerConfigOptions.SNOWFLAKE.key().equals(name)) {
                engineConfig.setSnowflakeConfig(parseSnowflakeConfig(node));
            } else if (ServerConfigOptions.NACOS.key().equals(name)) {
                engineConfig.setNacosConfig(parseNacosConfig(node));
            } else if (ServerConfigOptions.CONSUL.key().equals(name)) {
                engineConfig.setConsulConfig(parseConsulConfig(node));
            } else if (ServerConfigOptions.HOS.key().equals(name)) {
                engineConfig.setHosConfig(parseHosConfig(node));
            } else if (ServerConfigOptions.ZOOKEEPER.key().equals(name)) {
                engineConfig.setZookeeperConfig(parseZookeeperConfig(node));
            } else if (ServerConfigOptions.HDFS.key().equals(name)) {
                engineConfig.setHdfsConfig(parseHdfsConfig(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);

            }

        }
    }

    private HosConfig parseHosConfig(Node hosNode) {
        HosConfig hosConfig = new HosConfig();
        for (Node node : childElements(hosNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.HOS_TOKEN.key().equals(name)) {
                hosConfig.setToken(getTextContent(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return hosConfig;
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

    private SnowflakeConfig parseSnowflakeConfig(Node snowflakeNode) {
        SnowflakeConfig snowflakeConfig = new SnowflakeConfig();
        for (Node node : childElements(snowflakeNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.SNOWFLAKE_DATA_CENTER_ID.key().equals(name)) {
                snowflakeConfig.setDataCenterId(getTextContent(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return snowflakeConfig;
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

    private KnowledgeBaseConfig parseKnowledgeBaseConfig(Node knowledgeBaseNode) {
        KnowledgeBaseConfig knowledgeBaseConfig = new KnowledgeBaseConfig();
        for (Node node : childElements(knowledgeBaseNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.KNOWLEDGE_BASE_FILE_CHECK_NUMBER.key().equals(name)) {
                knowledgeBaseConfig.setFileCheckNumber(getIntegerValue(ServerConfigOptions.KNOWLEDGE_BASE_FILE_CHECK_NUMBER.key(), getTextContent(node)));
            } else if (ServerConfigOptions.KNOWLEDGE_BASE_ASN_LOOKUP.key().equals(name)) {
                knowledgeBaseConfig.setAsnLookupConfigList(parseAsnLookupConfigList(node));
            } else if (ServerConfigOptions.KNOWLEDGE_BASE_STORAGE.key().equals(name)) {
                knowledgeBaseConfig.setStorageConfig(parseStorageConfig(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return knowledgeBaseConfig;
    }

    private StorageConfig parseStorageConfig(Node storageNode) {
        StorageConfig storageConfig = new StorageConfig();
        for (Node node : childElements(storageNode)) {
            String name = cleanNodeName(node);
            if (ServerConfigOptions.KNOWLEDGE_BASE_STORAGE_FS_TYPE.key().equals(name)) {
                storageConfig.setFsType(getTextContent(node));
            } else if (ServerConfigOptions.KNOWLEDGE_BASE_STORAGE_FS_DEFAULT_PATH.key().equals(name)) {
                storageConfig.setFsDefaultPath(getTextContent(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return storageConfig;
    }

    private List<AsnLookupConfig> parseAsnLookupConfigList(Node asnLookupListNode) {
        List<AsnLookupConfig> asnLookupConfigList = new ArrayList<>();
        for (Node node : childElements(asnLookupListNode)) {
            String name = cleanNodeName(node);
            asnLookupConfigList.add(parseAsnLookupConfig(node));
        }
        return asnLookupConfigList;
    }

   private AsnLookupConfig parseAsnLookupConfig(Node asnLookupNode) {
        AsnLookupConfig asnLookupConfig = new AsnLookupConfig();
        for (Node node : childElements(asnLookupNode)) {
            String name = cleanNodeName(node);

            if (ServerConfigOptions.ASN_LOOKUP_VENDOR_ID.key().equals(name)) {
                asnLookupConfig.setVendorId(getTextContent(node));
            } else if (ServerConfigOptions.ASN_LOOKUP_DEFAULT_MMDB_FILE.key().equals(name)) {
                asnLookupConfig.setDefaultMMDBFile(getTextContent(node));
            } else if (ServerConfigOptions.ASN_LOOKUP_USER_DEFINED_MMDB_FILE.key().equals(name)) {
                asnLookupConfig.setUserDefinedMMDBFile(getTextContent(node));
            } else {
                LOGGER.warning("Unrecognized configuration element: " + name);
            }
        }
        return asnLookupConfig;
    }


}
