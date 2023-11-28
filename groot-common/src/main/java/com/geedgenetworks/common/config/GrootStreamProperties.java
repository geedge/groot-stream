package com.geedgenetworks.common.config;

import com.hazelcast.spi.properties.HazelcastProperty;

/** Defines the names and default values for internal Hazelcast GrootStream properties. */
public final class GrootStreamProperties {
    public static final HazelcastProperty GROOTSTREAM_HOME =
            new HazelcastProperty("grootstream.home", "");

    private GrootStreamProperties() {}
}
