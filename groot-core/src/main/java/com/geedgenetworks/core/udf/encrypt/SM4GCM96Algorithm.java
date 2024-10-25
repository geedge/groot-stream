package com.geedgenetworks.core.udf.encrypt;

import cn.hutool.core.util.RandomUtil;
import com.geedgenetworks.core.pojo.KmsKey;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class SM4GCM96Algorithm implements EncryptionAlgorithm {
    private static final String IDENTIFIER = "sm4-gcm96";
    private static final String ALGORITHM = "SM4";
    private static final String TRANSFORMATION = "SM4/GCM/NoPadding";
    private static final int GCM_TAG_LENGTH = 128;
    private static final int GCM_96_NONCE_LENGTH = 12;
    private static final int SECRET_KEY_LENGTH = 16;

    private final Cipher cipher;
    private KmsKey kmsKey;

    public SM4GCM96Algorithm() throws Exception {
        this.cipher = Cipher.getInstance(TRANSFORMATION);
    }

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public int getSecretKeyLength() {
        return SECRET_KEY_LENGTH;
    }

    @Override
    public KmsKey getKmsKey() {
        return kmsKey;
    }

    @Override
    public void setKmsKey(KmsKey kmsKey) {
        this.kmsKey = kmsKey;
    }

    @Override
    public String encrypt(String content) {
        String encryptedString = "";
        try {
            byte[] nonce = RandomUtil.randomBytes(GCM_96_NONCE_LENGTH);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(kmsKey.getKeyData(), ALGORITHM), gcmSpec);
            byte[] encryptedBytes = cipher.doFinal(content.getBytes());
            byte[] combinedBytes = new byte[GCM_96_NONCE_LENGTH + encryptedBytes.length];
            System.arraycopy(nonce, 0, combinedBytes, 0, GCM_96_NONCE_LENGTH);
            System.arraycopy(encryptedBytes, 0, combinedBytes, GCM_96_NONCE_LENGTH, encryptedBytes.length);
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
            byte[] nonce = RandomUtil.randomBytes(GCM_96_NONCE_LENGTH);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(kmsKey.getKeyData(), ALGORITHM), gcmSpec);
            byte[] combined = Base64.getDecoder().decode(content);
            byte[] encryptedBytes = new byte[combined.length - GCM_96_NONCE_LENGTH];
            System.arraycopy(combined, 0, nonce, 0, GCM_96_NONCE_LENGTH);
            System.arraycopy(combined, GCM_96_NONCE_LENGTH, encryptedBytes, 0, encryptedBytes.length);
            byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
            decryptedString = new String(decryptedBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return decryptedString;
    }
}
