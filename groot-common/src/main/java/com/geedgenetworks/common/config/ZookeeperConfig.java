package com.geedgenetworks.common.config;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

public class ZookeeperConfig implements Serializable {
    private String quorum = ServerConfigOptions.ZOOKEEPER_QUORUM.defaultValue();

    public void setQuorum(String quorum) {
        checkNotNull(quorum, ServerConfigOptions.ZOOKEEPER_QUORUM + "quorum should not be null");
        this.quorum = quorum;
    }

}
