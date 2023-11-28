package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;

@Data
public class HttpConPoolConfig implements Serializable {

    public static final long MIN_TIMEOUT_MS = 10;
    public int maxTotal = ServerConfigOptions.HTTP_CON_POOL_MAX_TOTAL.defaultValue();
    public int maxPerRoute = ServerConfigOptions.HTTP_CON_POOL_MAX_PER_ROUTE.defaultValue();
    public int connectionRequestTimeout = ServerConfigOptions.HTTP_CON_POOL_CONNECTION_REQUEST_TIMEOUT.defaultValue();
    public int connectTimeout = ServerConfigOptions.HTTP_CON_POOL_CONNECT_TIMEOUT.defaultValue();
    public int socketTimeout = ServerConfigOptions.HTTP_CON_POOL_SOCKET_TIMEOUT.defaultValue();

    public void setSocketTimeout(int socketTimeout) {
        checkArgument(socketTimeout > MIN_TIMEOUT_MS, ServerConfigOptions.HTTP_CON_POOL_SOCKET_TIMEOUT + "socketTimeout should be greater than " + MIN_TIMEOUT_MS);
        this.socketTimeout = socketTimeout;
    }
}
