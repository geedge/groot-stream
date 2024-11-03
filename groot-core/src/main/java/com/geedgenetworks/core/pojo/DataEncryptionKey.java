package com.geedgenetworks.core.pojo;


import lombok.Data;

@Data
public class DataEncryptionKey {

    private byte[] data;
    private int version;

    public DataEncryptionKey() {
    }

    public DataEncryptionKey(byte[] data, int version) {
        this.data = data;
        this.version = version;
    }
}