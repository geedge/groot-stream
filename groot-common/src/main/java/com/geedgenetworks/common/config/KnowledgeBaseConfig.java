package com.geedgenetworks.common.config;

import com.geedgenetworks.utils.StringUtil;
import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

@Data
public class KnowledgeBaseConfig implements Serializable {
    private String name = CommonConfigOptions.KNOWLEDGE_BASE_NAME.defaultValue();
    private String fsType = CommonConfigOptions.KNOWLEDGE_BASE_FS_TYPE.defaultValue();
    private String fsPath = CommonConfigOptions.KNOWLEDGE_BASE_FS_PATH.defaultValue();
    private Map<String, String> properties = CommonConfigOptions.KNOWLEDGE_BASE_PROPERTIES.defaultValue();
    private List<String> files = CommonConfigOptions.KNOWLEDGE_BASE_FILES.defaultValue();

    public void setFsType(String fsType) {
        this.fsType = fsType;
    }

    public void setFsPath(String fsPath) {
        this.fsPath = fsPath;
    }

    public void setFiles(List<String> files) {
        this.files = files;
    }



}
