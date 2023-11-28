package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class NacosConfig implements Serializable {
    private String serverAddr = ServerConfigOptions.NACOS_SERVER_ADDR.defaultValue();
    private String userName = ServerConfigOptions.NACOS_USERNAME.defaultValue();
    private String password = ServerConfigOptions.NACOS_PASSWORD.defaultValue();
    private String namespace = ServerConfigOptions.NACOS_NAMESPACE.defaultValue();
    private String dataId = ServerConfigOptions.NACOS_DATA_ID.defaultValue();
    private String group = ServerConfigOptions.NACOS_GROUP.defaultValue();
    private int readTimeout = ServerConfigOptions.NACOS_READ_TIMEOUT.defaultValue();

    public void setReadTimeout(int readTimeout) {
        checkArgument(readTimeout > 0, ServerConfigOptions.NACOS_READ_TIMEOUT + "readTimeout should be greater than 0");
        this.readTimeout = readTimeout;
    }

    public void setServerAddr(String serverAddr) {
        checkNotNull(serverAddr, ServerConfigOptions.NACOS_SERVER_ADDR + "serverAddr should not be null");
        this.serverAddr = serverAddr;
    }

    public void setDataId(String dataId) {
        checkNotNull(dataId, ServerConfigOptions.NACOS_DATA_ID + "dataId should not be null");
        this.dataId = dataId;
    }
}
