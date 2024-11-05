package com.geedgenetworks.core.udf.encrypt;

import cn.hutool.core.util.RandomUtil;
import com.geedgenetworks.core.pojo.DataEncryptionKey;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class AES128GCM implements Crypto {
    private static final String IDENTIFIER = "aes-128-gcm";
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_TAG_LENGTH = 128;
    private static final int GCM_96_NONCE_LENGTH = 12;
    private static final int SECRET_KEY_LENGTH = 16;
    private static final byte[] DEFAULT_SECRET_KEY = ".geedgenetworks.".getBytes();
    private static final byte[] NONCE = RandomUtil.randomBytes(GCM_96_NONCE_LENGTH);

    private DataEncryptionKey dek;

    public AES128GCM() {
        this.dek = new DataEncryptionKey(DEFAULT_SECRET_KEY, 1);
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
    public DataEncryptionKey getDataEncryptionKey() {
        return dek;
    }

    @Override
    public void setDataEncryptionKey(DataEncryptionKey dek) {
        this.dek = dek;
    }

    @Override
    public String encrypt(String content) {
        String encryptedString = "";
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, NONCE);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek.getData(), ALGORITHM), gcmSpec);
            byte[] encryptedBytes = cipher.doFinal(content.getBytes());
            byte[] combinedBytes = new byte[GCM_96_NONCE_LENGTH + encryptedBytes.length];
            System.arraycopy(NONCE, 0, combinedBytes, 0, GCM_96_NONCE_LENGTH);
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
            byte[] combined = Base64.getDecoder().decode(content);
            byte[] encryptedBytes = new byte[combined.length - GCM_96_NONCE_LENGTH];
            System.arraycopy(combined, 0, NONCE, 0, GCM_96_NONCE_LENGTH);
            System.arraycopy(combined, GCM_96_NONCE_LENGTH, encryptedBytes, 0, encryptedBytes.length);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, NONCE);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek.getData(), ALGORITHM), gcmSpec);
            byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
            decryptedString = new String(decryptedBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return decryptedString;
    }
}
