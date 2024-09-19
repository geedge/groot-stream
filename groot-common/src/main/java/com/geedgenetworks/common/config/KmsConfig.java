package com.geedgenetworks.common.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class KmsConfig implements Serializable {

    private String type =  CommonConfigOptions.KMS_TYPE.defaultValue();
    private String url = CommonConfigOptions.KMS_URL.defaultValue();
    private String token = CommonConfigOptions.KMS_TOKEN.defaultValue();
    private String keyPath = CommonConfigOptions.KMS_KEY_PATH.defaultValue();



}
