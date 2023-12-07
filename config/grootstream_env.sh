#!/usr/bin/env bash
# Home directory of flink distribution.
FLINK_HOME=${FLINK_HOME:-/opt/flink}
FLINK_JOB_MANAGER_ADDRESS=${FLINK_JOB_MANAGER_ADDRESS:-localhost:8081}
YARN_ADDRESS=${YARN_ADDRESS:-yarn-cluster}