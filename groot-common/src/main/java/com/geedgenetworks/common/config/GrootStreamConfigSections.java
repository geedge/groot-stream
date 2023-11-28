package com.geedgenetworks.common.config;

/** Configuration sections for Hazelcast GrootStream shared by YAML based configurations */
enum GrootStreamConfigSections {
    GROOTSTREAM("grootstream", false),
    ENGINE("engine", false);

    final String name;
    final boolean multipleOccurrence;

    GrootStreamConfigSections(String name, boolean multipleOccurrence) {
        this.name = name;
        this.multipleOccurrence = multipleOccurrence;
    }

    static boolean canOccurMultipleTimes(String name) {
        for (GrootStreamConfigSections element : values()) {
            if (name.equals(element.name)) {
                return element.multipleOccurrence;
            }
        }
        return false;
    }

    boolean isEqual(String name) {
        return this.name.equals(name);
    }
}
