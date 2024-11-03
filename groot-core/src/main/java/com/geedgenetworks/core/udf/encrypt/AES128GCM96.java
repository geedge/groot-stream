package com.geedgenetworks.core.udf.encrypt;

import cn.hutool.core.util.RandomUtil;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.DataEncryptionKey;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class AES128GCM96 implements Crypto {
    private static final String IDENTIFIER = "aes-128-gcm96";
    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int GCM_TAG_LENGTH = 128;
    private static final int GCM_96_NONCE_LENGTH = 12;
    private static final int SECRET_KEY_LENGTH = 16;
    private static final byte[] DEFAULT_SECRET_KEY = ".geedgenetworks.".getBytes();

    private final Cipher cipher;
    private DataEncryptionKey dek;

    public AES128GCM96() {
        try {
            this.cipher = Cipher.getInstance(TRANSFORMATION);
            this.dek = new DataEncryptionKey(DEFAULT_SECRET_KEY, 1);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Failed to initialize AES128GCM96", e);
        }
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
            byte[] nonce = RandomUtil.randomBytes(GCM_96_NONCE_LENGTH);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek.getData(), ALGORITHM), gcmSpec);
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
            byte[] combined = Base64.getDecoder().decode(content);
            byte[] encryptedBytes = new byte[combined.length - GCM_96_NONCE_LENGTH];
            System.arraycopy(combined, 0, nonce, 0, GCM_96_NONCE_LENGTH);
            System.arraycopy(combined, GCM_96_NONCE_LENGTH, encryptedBytes, 0, encryptedBytes.length);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, nonce);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek.getData(), ALGORITHM), gcmSpec);
            byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
            decryptedString = new String(decryptedBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return decryptedString;
    }
}
