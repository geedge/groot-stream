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
    private String type = CommonConfigOptions.KNOWLEDGE_BASE_TYPE.defaultValue();
    private Map<String, String> properties = CommonConfigOptions.KNOWLEDGE_BASE_PROPERTIES.defaultValue();
    private List<String> files = CommonConfigOptions.KNOWLEDGE_BASE_FILES.defaultValue();

    public void setType(String type) {
        checkArgument(StringUtil.isNotBlank(type) && Arrays.asList("asnlookup", "geoiplookup").contains(type.toLowerCase())
                , CommonConfigOptions.KNOWLEDGE_BASE_TYPE + "Type should be asnlookup or geoiplookup");
        this.type = type;
    }

    public void setFiles(List<String> files) {
        checkArgument(files != null && files.size() > 0,
                CommonConfigOptions.KNOWLEDGE_BASE_FILES + "files should not be null");
        this.files = files;
    }



}
