package com.geedgenetworks.bootstrap.enums;

public enum ProcessorType {
    SOURCE("source"),
    FILTER("filter"),
    SPLIT("split"),
    PREPROCESSING("preprocessing"),
    PROCESSING("processing"),
    POSTPROCESSING("postprocessing"),
    SINK("sink");

    private final String type;

    ProcessorType(String type) {this.type = type;}

    public String getType() {
        return type;
    }
}
