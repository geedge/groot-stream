package com.geedgenetworks.common.exception;

public interface GrootStreamErrorCodeSupplier {

    String getCode();

    String getDescription();

    default String getErrorMessage() {
        return String.format("ErrorCode: [%s], ErrorMessage: [%s]", getCode(), getDescription());
    }


}
