package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *  Describe the common config of groot stream (grootstream.yaml).
 */
@Data
public class CommonConfig implements Serializable {

    private List<KnowledgeBaseConfig> knowledgeBaseConfig = CommonConfigOptions.KNOWLEDGE_BASE.defaultValue();

    private Map<String,KmsConfig> kmsConfig = CommonConfigOptions.KMS.defaultValue();

    private Map<String,String> propertiesConfig = CommonConfigOptions.PROPERTIES.defaultValue();

    public void setKnowledgeBaseConfig(List<KnowledgeBaseConfig> knowledgeBaseConfig) {
        checkNotNull(knowledgeBaseConfig, CommonConfigOptions.KNOWLEDGE_BASE + " knowledgeConfig should not be null");
        this.knowledgeBaseConfig = knowledgeBaseConfig;
    }

   public void setKmsConfig(Map<String,KmsConfig> kmsConfig) {
       checkNotNull(kmsConfig, CommonConfigOptions.KMS + " kmsConfig should not be null");
       this.kmsConfig = kmsConfig;
   }





}
