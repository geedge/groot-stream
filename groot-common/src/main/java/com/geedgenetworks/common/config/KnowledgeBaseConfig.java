package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class KnowledgeBaseConfig implements Serializable {
    private int fileCheckNumber = ServerConfigOptions.KNOWLEDGE_BASE_FILE_CHECK_NUMBER.defaultValue();
    private List<AsnLookupConfig> asnLookupConfigList = ServerConfigOptions.KNOWLEDGE_BASE_ASN_LOOKUP.defaultValue();
    private StorageConfig storageConfig = ServerConfigOptions.KNOWLEDGE_BASE_STORAGE.defaultValue();



}
