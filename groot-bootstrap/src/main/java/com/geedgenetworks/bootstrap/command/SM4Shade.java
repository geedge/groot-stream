package com.geedgenetworks.bootstrap.command;

import cn.hutool.crypto.KeyUtil;
import cn.hutool.crypto.SmUtil;
import cn.hutool.crypto.symmetric.SM4;

import java.nio.charset.StandardCharsets;

public class SM4Shade implements CryptoShade {
    private static final String IDENTIFIER = "sm4";

    private static final String[] SENSITIVE_OPTIONS =
            new String[] {"connection.user", "connection.password", "kafka.sasl.jaas.config","kafka.ssl.keystore.password","kafka.ssl.truststore.password","kafka.ssl.key.password"};

    private static final byte[] SECURITY_KEY = KeyUtil.generateKey(SM4.ALGORITHM_NAME, ".geedgenetworks.".getBytes(StandardCharsets.UTF_8)).getEncoded();

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
        return SmUtil.sm4(SECURITY_KEY).encryptBase64(content, StandardCharsets.UTF_8);
    }

    @Override
    public String decrypt(String content) {
        return SmUtil.sm4(SECURITY_KEY).decryptStr(content, StandardCharsets.UTF_8);
    }
}
