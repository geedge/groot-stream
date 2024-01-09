package com.geedgenetworks.core.connector.print;

public enum PrintMode {
    STDOUT, LOG_INFO, LOG_WARN, NULL;

    public static PrintMode fromName(String name) {
        for (PrintMode mode : values()) {
            if (mode.name().equalsIgnoreCase(name)) {
                return mode;
            }
        }
        return null;
    }
}
