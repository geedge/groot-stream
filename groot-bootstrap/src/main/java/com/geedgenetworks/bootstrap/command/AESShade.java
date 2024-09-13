package com.geedgenetworks.bootstrap.command;

import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.symmetric.SymmetricAlgorithm;
import com.geedgenetworks.common.crypto.CryptoShade;

import java.nio.charset.StandardCharsets;

public class AESShade implements CryptoShade {
    private static final String IDENTIFIER = "aes";

    private static final byte[] SECURITY_KEY =
            SecureUtil.generateKey(SymmetricAlgorithm.AES.getValue(), ".geedgenetworks.".getBytes(StandardCharsets.UTF_8)).getEncoded() ;

    private static final String[] SENSITIVE_OPTIONS =
            new String[] {"connection.user", "connection.password", "kafka.sasl.jaas.config","kafka.ssl.keystore.password","kafka.ssl.truststore.password","kafka.ssl.key.password"};

    @Override
    public String[] sensitiveOptions() {
        return SENSITIVE_OPTIONS;
    }

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public String encrypt(String content) {
        return SecureUtil.aes(SECURITY_KEY).encryptHex(content, StandardCharsets.UTF_8);
    }

    @Override
    public String decrypt(String content) {
        return SecureUtil.aes(SECURITY_KEY).decryptStr(content, StandardCharsets.UTF_8);
    }
}
