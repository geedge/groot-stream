package com.geedgenetworks.common.exception;

public class IMapStorageException extends RuntimeException {
    public IMapStorageException(String message) {
        super(message);
    }

    public IMapStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public IMapStorageException(Throwable cause) {
        super(cause);
    }

    public IMapStorageException(Throwable cause, String message, Object... data) {
        super(String.format(message, data), cause);
    }
}
