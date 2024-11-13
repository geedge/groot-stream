package com.geedgenetworks.bootstrap.command;

public interface CryptoShade {

    /**
     * The unique identifier of the current interface, used it to select the correct {@link
     * CryptoShade}
     */
    String getIdentifier();

    /**
     * Encrypt the content
     *
     * @param content The content to encrypt
     */
    String encrypt(String content);

    /**
     * Decrypt the content
     *
     * @param content The content to decrypt
     */
    String decrypt(String content);

    /** To expand the options that user want to encrypt */
    default String[] sensitiveOptions() {
        return new String[0];
    }
}
