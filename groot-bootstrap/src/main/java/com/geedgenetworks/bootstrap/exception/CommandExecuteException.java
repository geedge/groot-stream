package com.geedgenetworks.bootstrap.exception;

public class CommandExecuteException extends RuntimeException {
    public CommandExecuteException(String message) {
        super(message);
    }

    public CommandExecuteException(String message, Throwable cause) {
        super(message, cause);
    }
}
