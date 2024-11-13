package com.geedgenetworks.bootstrap.command;

import cn.hutool.core.util.RandomUtil;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Base64;

public class AES128GCM96Shade implements CryptoShade {
    private static final String IDENTIFIER = "aes-128-gcm96";
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_TAG_LENGTH = 128;
    private static final int GCM_NONCE_LENGTH = 12;
    private static final byte[] NONCE = RandomUtil.randomBytes(GCM_NONCE_LENGTH);

    private static final String[] SENSITIVE_OPTIONS =
            new String[]{"secret_key", "connection.user", "connection.password", "kafka.sasl.jaas.config", "kafka.ssl.keystore.password", "kafka.ssl.truststore.password", "kafka.ssl.key.password"};

    private static final Key SECURITY_KEY = new SecretKeySpec(".geedgenetworks.".getBytes(StandardCharsets.UTF_8), ALGORITHM);

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
        String encryptedString = "";
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, NONCE);
            cipher.init(Cipher.ENCRYPT_MODE, SECURITY_KEY, gcmSpec);
            byte[] encryptedBytes = cipher.doFinal(content.getBytes());
            byte[] combinedBytes = new byte[GCM_NONCE_LENGTH + encryptedBytes.length];
            System.arraycopy(NONCE, 0, combinedBytes, 0, GCM_NONCE_LENGTH);
            System.arraycopy(encryptedBytes, 0, combinedBytes, GCM_NONCE_LENGTH, encryptedBytes.length);
            encryptedString = Base64.getEncoder().encodeToString(combinedBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return encryptedString;
    }

    @Override
    public String decrypt(String content) {
        String decryptedString = "";
        try {
            byte[] combined = Base64.getDecoder().decode(content);
            byte[] encryptedBytes = new byte[combined.length - GCM_NONCE_LENGTH];
            System.arraycopy(combined, 0, NONCE, 0, GCM_NONCE_LENGTH);
            System.arraycopy(combined, GCM_NONCE_LENGTH, encryptedBytes, 0, encryptedBytes.length);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, NONCE);
            cipher.init(Cipher.DECRYPT_MODE, SECURITY_KEY, gcmSpec);
            byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
            decryptedString = new String(decryptedBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return decryptedString;
    }
}
