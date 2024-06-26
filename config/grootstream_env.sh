#!/usr/bin/env bash
# Home directory of flink distribution.
FLINK_HOME=${FLINK_HOME:-/opt/flink}
# Flink job manager address deployed in standalone mode.
STANDALONE_JOB_MANAGER_ADDRESS=${STANDALONE_JOB_MANAGER_ADDRESS:-localhost:8081}
# YARN session cluster ID.
YARN_SESSION_CLUSTER_ID=${YARN_SESSION_CLUSTER_ID:-applicaion_1630480000000_0001}