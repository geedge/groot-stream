package com.geedgenetworks.common.exception;

public class ConfigValidationException extends GrootStreamRuntimeException {
    public ConfigValidationException(GrootStreamErrorCodeSupplier grootStreamErrorCodeSupplier, String errorMessage) {
        super(grootStreamErrorCodeSupplier, errorMessage);
    }

    public ConfigValidationException(GrootStreamErrorCodeSupplier grootStreamErrorCodeSupplier, String errorMessage, Throwable cause) {
        super(grootStreamErrorCodeSupplier, errorMessage, cause);
    }

    public ConfigValidationException(GrootStreamErrorCodeSupplier grootStreamErrorCodeSupplier, Throwable cause) {
        super(grootStreamErrorCodeSupplier, cause);
    }
}
