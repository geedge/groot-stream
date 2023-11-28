package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *  Describe the common config of groot stream (groot.yaml).
 */
@Data
public class EngineConfig implements Serializable {

    private int tickTupleFreqSecs = ServerConfigOptions.TICK_TUPLE_FREQ_SECS.defaultValue();
    private int gtpcScanMaxRows = ServerConfigOptions.GTPC_SCAN_MAX_ROWS.defaultValue();
    private int radiusScanMaxRows = ServerConfigOptions.RADIUS_SCAN_MAX_ROWS.defaultValue();
    private String radiusTableName = ServerConfigOptions.RADIUS_TABLE_NAME.defaultValue();
    private String gtpcTableName = ServerConfigOptions.GTPC_TABLE_NAME.defaultValue();
    private int hbaseRpcTimeout = ServerConfigOptions.HBASE_RPC_TIMEOUT.defaultValue();
    private HttpConPoolConfig httpConPoolConfig = ServerConfigOptions.HTTP_CON_POOL.defaultValue();
    private KnowledgeBaseConfig knowledgeBaseConfig = ServerConfigOptions.KNOWLEDGE_BASE.defaultValue();
    private SnowflakeConfig snowflakeConfig = ServerConfigOptions.SNOWFLAKE.defaultValue();
    private NacosConfig nacosConfig = ServerConfigOptions.NACOS.defaultValue();
    private ConsulConfig consulConfig = ServerConfigOptions.CONSUL.defaultValue();
    private HosConfig hosConfig = ServerConfigOptions.HOS.defaultValue();
    private ZookeeperConfig zookeeperConfig = ServerConfigOptions.ZOOKEEPER.defaultValue();
    private HdfsConfig hdfsConfig = ServerConfigOptions.HDFS.defaultValue();



    public void setKnowledgeBaseConfig(KnowledgeBaseConfig knowledgeBaseConfig) {
        checkNotNull(knowledgeBaseConfig, ServerConfigOptions.KNOWLEDGE_BASE + "knowledgeConfig should not be null");
        this.knowledgeBaseConfig = knowledgeBaseConfig;
    }





}
