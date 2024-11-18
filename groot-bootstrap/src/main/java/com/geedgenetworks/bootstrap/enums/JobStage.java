package com.geedgenetworks.bootstrap.enums;

public enum JobStage {
    SOURCE("source"),
    FILTER("filter"),
    SPLIT("split"),
    @Deprecated
    PREPROCESSING("preprocessing"),
    PROCESSING("processing"),
    @Deprecated
    POSTPROCESSING("postprocessing"),
    SINK("sink");

    private final String type;
    public String getType() {
        return type;
    }
    JobStage(String type) {this.type = type;}

    public static JobStage fromType(String type) {
        for (JobStage stage : values()) {
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
