package com.geedgenetworks.core.pojo;


import lombok.Data;

@Data
public class KmsKey {

    private byte[] keyData;
    private int keyVersion;

    public KmsKey() {
    }

    public KmsKey(byte[] keyData, int keyVersion) {
        this.keyData = keyData;
        this.keyVersion = keyVersion;
    }
}