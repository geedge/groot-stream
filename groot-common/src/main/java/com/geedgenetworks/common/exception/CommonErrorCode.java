package com.geedgenetworks.common.exception;

public enum CommonErrorCode implements GrootStreamErrorCodeSupplier {

    UNSUPPORTED_OPERATION("GROOT-STREAM-COMMON-0001", "Unsupported operation exception"),
    ILLEGAL_ARGUMENT("GROOT-STREAM-COMMON-0002", "Illegal argument exception"),
    SYNTAX_ERROR("GROOT-STREAM-COMMON-0003", "Syntax Error"),
    FILE_OPERATION_ERROR("GROOT-STREAM-COMMON-0004", "File operation failed, such as (read,list,write,move,copy,sync) etc...");

    private final String code;
    private final String description;

    CommonErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }
}
