package com.geedgenetworks.common.config;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class KnowledgeBaseConfig implements Serializable {
    private String name = CommonConfigOptions.KNOWLEDGE_BASE_NAME.defaultValue();
    private String fsType = CommonConfigOptions.KNOWLEDGE_BASE_FS_TYPE.defaultValue();
    private String fsPath = CommonConfigOptions.KNOWLEDGE_BASE_FS_PATH.defaultValue();
    private Map<String, String> properties = CommonConfigOptions.KNOWLEDGE_BASE_PROPERTIES.defaultValue();
    private List<String> files = CommonConfigOptions.KNOWLEDGE_BASE_FILES.defaultValue();

}
