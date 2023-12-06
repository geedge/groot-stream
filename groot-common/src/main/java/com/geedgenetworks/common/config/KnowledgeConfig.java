package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class KnowledgeConfig implements Serializable {
    private String name = ServerConfigOptions.KNOWLEDGE_BASE_NAME.defaultValue();
    private String type = ServerConfigOptions.KNOWLEDGE_BASE_TYPE.defaultValue();
    private Map<String, String> properties = ServerConfigOptions.KNOWLEDGE_BASE_PROPERTIES.defaultValue();
    private List<String> files = ServerConfigOptions.KNOWLEDGE_BASE_FILES.defaultValue();
}
