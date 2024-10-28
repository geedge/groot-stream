package com.geedgenetworks.core.utils;

import com.geedgenetworks.core.udf.encrypt.EncryptionAlgorithm;
import com.geedgenetworks.core.udf.encrypt.AES128GCM96Algorithm;
import com.geedgenetworks.core.udf.encrypt.AES256GCM96Algorithm;
import com.geedgenetworks.core.udf.encrypt.SM4GCM96Algorithm;
import lombok.extern.slf4j.Slf4j;

/**
 * Crypto shade utilities
 */
@Slf4j
public final class EncryptionAlgorithmUtils {
    public static final String ALGORITHM_AES_128_GCM96_NAME = "aes-128-gcm96";
    public static final String ALGORITHM_AES_256_GCM96_NAME = "aes-256-gcm96";
    public static final String ALGORITHM_SM4_GCM96_NAME = "sm4-gcm96";

    public static EncryptionAlgorithm createEncryptionAlgorithm(String identifier) throws Exception {
        switch (identifier) {
            case ALGORITHM_AES_128_GCM96_NAME:
                return new AES128GCM96Algorithm();
            case ALGORITHM_AES_256_GCM96_NAME:
                return new AES256GCM96Algorithm();
            case ALGORITHM_SM4_GCM96_NAME:
                return new SM4GCM96Algorithm();
            default:
                return null;
        }
    }
}
