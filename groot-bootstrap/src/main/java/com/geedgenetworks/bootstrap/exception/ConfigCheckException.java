package com.geedgenetworks.bootstrap.exception;

public class ConfigCheckException extends CommandException {
    public ConfigCheckException(String message) {
        super(message);
    }
    public ConfigCheckException(String message, Throwable cause) {
        super(message, cause);
    }
}
