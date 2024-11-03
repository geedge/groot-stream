package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class ClientSSLConfig implements Serializable {
    private Boolean skipVerification = CommonConfigOptions.SSL_SKIP_VERIFICATION.defaultValue();

    private String caCertificatePath = CommonConfigOptions.SSL_CA_CERTIFICATE_PATH.defaultValue();

    private String certificatePath = CommonConfigOptions.SSL_CERTIFICATE_PATH.defaultValue();

    private String privateKeyPath = CommonConfigOptions.SSL_PRIVATE_KEY_PATH.defaultValue();
}
