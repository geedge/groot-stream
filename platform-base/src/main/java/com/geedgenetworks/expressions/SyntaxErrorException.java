package com.geedgenetworks.expressions;

/**
 * Exception class representing a syntax error that may occur during program execution.
 *
 * @author chaoc
 * @since 1.0
 */
public class SyntaxErrorException extends RuntimeException {

    public SyntaxErrorException() {
    }

    public SyntaxErrorException(String message) {
        super(message);
    }
}