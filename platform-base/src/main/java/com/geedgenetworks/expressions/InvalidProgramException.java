package com.geedgenetworks.expressions;

/**
 * Program codegen error.
 *
 * @author chaoc
 * @since 1.0
 */
public class InvalidProgramException extends RuntimeException {

    public InvalidProgramException() {
    }

    public InvalidProgramException(String message) {
        super(message);
    }
}