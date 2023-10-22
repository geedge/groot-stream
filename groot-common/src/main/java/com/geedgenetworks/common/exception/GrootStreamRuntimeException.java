package com.geedgenetworks.common.exception;

public class GrootStreamRuntimeException extends RuntimeException {
    private final GrootStreamErrorCodeSupplier grootStreamErrorCode;

    public GrootStreamRuntimeException(GrootStreamErrorCodeSupplier grootStreamErrorCode, String errorMessage) {
        super(grootStreamErrorCode.getErrorMessage() + "-" + errorMessage);
        this.grootStreamErrorCode = grootStreamErrorCode;
    }

    public GrootStreamRuntimeException(GrootStreamErrorCodeSupplier grootStreamErrorCode, String errorMessage, Throwable cause) {
        super(grootStreamErrorCode.getErrorMessage() + "-" + errorMessage, cause);
        this.grootStreamErrorCode = grootStreamErrorCode;
    }

    public GrootStreamRuntimeException(GrootStreamErrorCodeSupplier grootStreamErrorCode, Throwable cause) {
        super(grootStreamErrorCode.getErrorMessage(), cause);
        this.grootStreamErrorCode = grootStreamErrorCode;
    }

}
