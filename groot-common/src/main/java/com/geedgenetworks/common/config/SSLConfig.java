package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class SSLConfig implements Serializable {
    private Boolean skipVerification = CommonConfigOptions.SKIP_VERIFICATION.defaultValue();

    private String caCertificatePath = CommonConfigOptions.CA_CERTIFICATE_PATH.defaultValue();

    private String certificatePath = CommonConfigOptions.CERTIFICATE_PATH.defaultValue();

    private String privateKeyPath = CommonConfigOptions.PRIVATE_KEY_PATH.defaultValue();
}
