package com.geedgenetworks.common.config;

import com.geedgenetworks.utils.StringUtil;
import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

@Data
public class StorageConfig implements Serializable {
    private String fsType = ServerConfigOptions.KNOWLEDGE_BASE_STORAGE_FS_TYPE.defaultValue();
    private String fsDefaultPath = ServerConfigOptions.KNOWLEDGE_BASE_STORAGE_FS_DEFAULT_PATH.defaultValue();

    public void setFsType(String fsType) {
        checkArgument(StringUtil.isNotBlank(fsType) && Arrays.asList("hdfs", "localfile").contains(fsType.toLowerCase())
                , ServerConfigOptions.KNOWLEDGE_BASE_STORAGE_FS_TYPE + "fsType should be hdfs or localfile");
        this.fsType = fsType;
    }
}
