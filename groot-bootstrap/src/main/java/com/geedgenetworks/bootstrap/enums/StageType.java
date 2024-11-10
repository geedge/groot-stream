package com.geedgenetworks.bootstrap.enums;

public enum StageType {
    SOURCE("source"),
    FILTER("filter"),
    SPLIT("split"),
    PREPROCESSING("preprocessing"),
    PROCESSING("processing"),
    POSTPROCESSING("postprocessing"),
    SINK("sink");

    private final String type;
    public String getType() {
        return type;
    }
    StageType(String type) {this.type = type;}

    public static StageType fromType(String type) {
        for (StageType stage : values()) {
            if (stage.type.equalsIgnoreCase(type)) {
                return stage;
            }
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    @Override
    public String toString() {
        return type;
    }

}
