package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class KmsConfig implements Serializable {
    private String type = CommonConfigOptions.KMS_TYPE.defaultValue();
    private String url = CommonConfigOptions.KMS_URL.defaultValue();
    private String username = CommonConfigOptions.KMS_USERNAME.defaultValue();
    private String password = CommonConfigOptions.KMS_PASSWORD.defaultValue();
    private String defaultKeyPath = CommonConfigOptions.KMS_DEFAULT_KEY_PATH.defaultValue();
    private String pluginKeyPath = CommonConfigOptions.KMS_PLUGIN_KEY_PATH.defaultValue();
}
