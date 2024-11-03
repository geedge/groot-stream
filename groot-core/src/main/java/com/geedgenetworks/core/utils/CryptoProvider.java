package com.geedgenetworks.core.utils;

import com.geedgenetworks.core.udf.encrypt.Crypto;
import com.geedgenetworks.core.udf.encrypt.AES128GCM96;
import com.geedgenetworks.core.udf.encrypt.AES256GCM96;
import com.geedgenetworks.core.udf.encrypt.SM4GCM96;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
public final class CryptoProvider {

    @Getter
    public enum Algorithm {
        AES_128_GCM96("aes-128-gcm96"),
        AES_256_GCM96("aes-256-gcm96"),
        SM4_GCM96("sm4-gcm96");

        private final String identifier;

        Algorithm(String identifier) {
            this.identifier = identifier;
        }

        public static Algorithm fromIdentifier(String identifier) {
            for (Algorithm algorithm : values()) {
                if (algorithm.identifier.equalsIgnoreCase(identifier)) {
                    return algorithm;
                }
            }
            throw new IllegalArgumentException("Unsupported algorithm identifier: " + identifier);
        }
    }

    private static final Map<String, Supplier<Crypto>> ALGORITHM_REGISTRY = new HashMap<>();

    static {
        ALGORITHM_REGISTRY.put(Algorithm.AES_128_GCM96.getIdentifier(), AES128GCM96::new);
        ALGORITHM_REGISTRY.put(Algorithm.AES_256_GCM96.getIdentifier(), AES256GCM96::new);
        ALGORITHM_REGISTRY.put(Algorithm.SM4_GCM96.getIdentifier(), SM4GCM96::new);
    }

    private CryptoProvider() {
        // Private constructor to prevent instantiation
    }

    public static Crypto createEncryptionAlgorithm(String identifier) {
        Supplier<Crypto> supplier = ALGORITHM_REGISTRY.get(identifier.toLowerCase());
        if (supplier == null) {
            throw new IllegalArgumentException("Unsupported algorithm identifier: " + identifier);
        }
        return supplier.get();  // Lazily creates a new instance when called
    }
}
