package com.geedgenetworks.core.udf.encrypt;

import com.geedgenetworks.core.pojo.KmsKey;

public interface EncryptionAlgorithm {
    String getIdentifier();

    int getSecretKeyLength();

    KmsKey getKmsKey();

    void setKmsKey(KmsKey kmsKey);

    String encrypt(String content);

    String decrypt(String content);
}
