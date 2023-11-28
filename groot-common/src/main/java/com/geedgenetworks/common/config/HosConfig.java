package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class HosConfig implements Serializable {
    public String token = ServerConfigOptions.HOS_TOKEN.defaultValue();

    public void setToken(String token) {
        checkNotNull(token, ServerConfigOptions.HOS_TOKEN + "token should not be null");
        this.token = token;
    }

}
