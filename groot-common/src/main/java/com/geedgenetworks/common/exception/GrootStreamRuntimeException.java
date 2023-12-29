package com.geedgenetworks.common.exception;

public class GrootStreamRuntimeException extends RuntimeException {
    private final GrootStreamErrorCodeSupplier grootStreamErrorCodeSupplier;

    public GrootStreamRuntimeException(GrootStreamErrorCodeSupplier grootStreamErrorCodeSupplier, String errorMessage) {
        super(grootStreamErrorCodeSupplier.getErrorMessage() + "-" + errorMessage);
        this.grootStreamErrorCodeSupplier = grootStreamErrorCodeSupplier;
    }

    public GrootStreamRuntimeException(GrootStreamErrorCodeSupplier grootStreamErrorCodeSupplier, String errorMessage, Throwable cause) {
        super(grootStreamErrorCodeSupplier.getErrorMessage() + "-" + errorMessage, cause);
        this.grootStreamErrorCodeSupplier = grootStreamErrorCodeSupplier;
    }

    public GrootStreamRuntimeException(GrootStreamErrorCodeSupplier grootStreamErrorCodeSupplier, Throwable cause) {
        super(grootStreamErrorCodeSupplier.getErrorMessage(), cause);
        this.grootStreamErrorCodeSupplier = grootStreamErrorCodeSupplier;
    }

}
