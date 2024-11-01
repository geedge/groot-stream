package com.geedgenetworks.bootstrap.command;

import com.geedgenetworks.common.crypto.CryptoShade;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class Base64Shade implements CryptoShade {
    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    private static final Base64.Decoder DECODER = Base64.getDecoder();

    private static final String IDENTIFIER = "base64";

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public String encrypt(String content) {
        return ENCODER.encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String decrypt(String content) {
        return new String(DECODER.decode(content));
    }
}
