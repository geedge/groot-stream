package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class ConsulConfig implements Serializable {
    public String serverAddr = ServerConfigOptions.CONSUL_SERVER_ADDR.defaultValue();
    public int serverPort = ServerConfigOptions.CONSUL_SERVER_PORT.defaultValue();
    public String token = ServerConfigOptions.CONSUL_TOKEN.defaultValue();

    public void setServerAddr(String serverAddr) {
        checkNotNull(serverAddr, ServerConfigOptions.CONSUL_SERVER_ADDR + "serverAddr should not be null");
        this.serverAddr = serverAddr;
    }

    public void setToken(String token) {
        checkNotNull(token, ServerConfigOptions.CONSUL_TOKEN + "token should not be null");
        this.token = token;
    }
}
