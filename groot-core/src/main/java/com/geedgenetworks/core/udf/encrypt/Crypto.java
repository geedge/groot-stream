package com.geedgenetworks.core.udf.encrypt;

import com.geedgenetworks.core.pojo.DataEncryptionKey;

public interface Crypto {
    String getIdentifier();

    int getSecretKeyLength();

    DataEncryptionKey getDataEncryptionKey();

    void setDataEncryptionKey(DataEncryptionKey dek);

    String encrypt(String content);

    String decrypt(String content);
}
