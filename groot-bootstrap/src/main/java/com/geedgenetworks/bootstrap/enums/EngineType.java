package com.geedgenetworks.bootstrap.enums;

public enum EngineType {
    FLINK13("flink", "groot-stream-bootstrap.jar", "start.sh");

    private final String engine;

    private final String jarName;

    private final String shellName;

    EngineType(String engine, String jarName, String shellName) {
        this.engine = engine;
        this.jarName = jarName;
        this.shellName = shellName;
    }

    public String getEngine() {
        return engine;
    }

    public String getJarName() {
        return jarName;
    }

    public String getShellName() {
        return shellName;
    }
}
