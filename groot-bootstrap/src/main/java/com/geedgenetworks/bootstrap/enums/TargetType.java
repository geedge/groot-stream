package com.geedgenetworks.bootstrap.enums;

public enum TargetType {

    LOCAL("local"),
    REMOTE("remote"),
    TEST("test"),
    YARN_SESSION("yarn-session"),
    YARN_PER_JOB("yarn-per-job"),
    YARN_APPLICATION("yarn-application");
    private final String target;

    TargetType(String target) {
        this.target = target;
    }

    public String getTarget() {
        return target;
    }
}
