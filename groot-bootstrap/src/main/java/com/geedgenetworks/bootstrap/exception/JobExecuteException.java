package com.geedgenetworks.bootstrap.exception;

public class JobExecuteException extends RuntimeException {
    public JobExecuteException(String message) {
        super(message);
    }

    public JobExecuteException(String message, Throwable cause) {
        super(message, cause);
    }
}
