package com.geedgenetworks.common.exception;

public enum CommonErrorCode implements GrootStreamErrorCodeSupplier {

    UNSUPPORTED_OPERATION("GROOT-STREAM-COMMON-0001", "Unsupported operation."),
    ILLEGAL_ARGUMENT("GROOT-STREAM-COMMON-0002", "Illegal argument."),
    SYNTAX_ERROR("GROOT-STREAM-COMMON-0003", "Syntax error."),
    FILE_OPERATION_ERROR("GROOT-STREAM-COMMON-0004", "File operation failed (e.g., read, list, write, move, copy, sync)."),
    CONFIG_VALIDATION_FAILED("GROOT-STREAM-COMMON-0005", "Configuration item validation failed."),
    JSON_OPERATION_FAILED(
            "GROOT-STREAM-COMMON-0006", "JSON convert/parse operation failed."),
    ;

    private final String code;
    private final String description;

    CommonErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
