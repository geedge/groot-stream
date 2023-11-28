package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class HdfsConfig implements Serializable {
    private String servers = ServerConfigOptions.HDFS_SERVERS.defaultValue();
}
