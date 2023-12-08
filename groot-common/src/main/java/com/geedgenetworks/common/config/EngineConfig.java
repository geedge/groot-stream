package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *  Describe the common config of groot stream (groot.yaml).
 */
@Data
public class EngineConfig implements Serializable {


    private HttpConPoolConfig httpConPoolConfig = ServerConfigOptions.HTTP_CON_POOL.defaultValue();
    private List<KnowledgeConfig> knowledgeBaseConfig = ServerConfigOptions.KNOWLEDGE_BASE.defaultValue();
    private NacosConfig nacosConfig = ServerConfigOptions.NACOS.defaultValue();
    private ConsulConfig consulConfig = ServerConfigOptions.CONSUL.defaultValue();
    private ZookeeperConfig zookeeperConfig = ServerConfigOptions.ZOOKEEPER.defaultValue();
    private HdfsConfig hdfsConfig = ServerConfigOptions.HDFS.defaultValue();


    public void setKnowledgeBaseConfig(List<KnowledgeConfig> knowledgeBaseConfig) {
        checkNotNull(knowledgeBaseConfig, ServerConfigOptions.KNOWLEDGE_BASE + "knowledgeConfig should not be null");
        this.knowledgeBaseConfig = knowledgeBaseConfig;
    }





}
