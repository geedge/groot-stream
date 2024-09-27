package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class SSLConfig implements Serializable {

    private Boolean enabled = CommonConfigOptions.SSL_ENABLED.defaultValue();

    private String certFile = CommonConfigOptions.SSL_CERT_FILE.defaultValue();

    private String keyFile = CommonConfigOptions.SSL_KEY_FILE.defaultValue();

    private Boolean requireClientAuth = CommonConfigOptions.SSL_REQUIRE_CLIENT_AUTH.defaultValue();


}
