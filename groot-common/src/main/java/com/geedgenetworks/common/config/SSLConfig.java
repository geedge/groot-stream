package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class SSLConfig implements Serializable {

    private Boolean disabled = CommonConfigOptions.SSL_DISABLED.defaultValue();

    private String certPath = CommonConfigOptions.SSL_CERT_PATH.defaultValue();

    private String privateKeyPath = CommonConfigOptions.SSL_PRIVATE_KEY_PATH.defaultValue();


}
