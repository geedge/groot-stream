package com.geedgenetworks.bootstrap.exception;

public class ConfigCheckException extends RuntimeException {
    public ConfigCheckException(String message) {
        super(message);
    }

    public ConfigCheckException(String message, Throwable cause) {
        super(message, cause);
    }
}
