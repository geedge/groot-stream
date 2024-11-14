package com.geedgenetworks.api.factory;

public interface Factory {
    /**
     * Returns the factory identifier.
     * If multiple factories exist for different versions, a version should be appended using "-".
     * (e.g. {@code kafka-1}).
     */
    String type();

    /**
     * Returns true if the factory supports the given type.
     */
    boolean supportsType(String type);


}
