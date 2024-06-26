#!/bin/bash

# Function to display usage
display_usage() {
    echo "Usage: $0 <DEPLOYMENT_MODE> <JOB_OR_YARN_APP_NAME>"
    echo "DEPLOYMENT_MODE: standalone, yarn-per-job, or yarn-session"
}
# Function to stop Flink jobs based on deployment mode
stop_jobs() {
    case $1 in
        standalone)
            "$FLINK_HOME"/bin/flink list -r -m "$FLINK_STANDALONE_JOB_MANAGER_ADDRESS" | grep "$job_or_yarn_app_name" | awk '{print $4}' | while read -r jobId
            do
                "$FLINK_HOME"/bin/flink cancel "$jobId" -m "$FLINK_STANDALONE_JOB_MANAGER_ADDRESS"
                echo "Stopped Flink job with JobID: $jobId"
            done
            ;;
        yarn-per-job)
           # Command to stop YARN applications for the specified Yarn cluster app name
            yarn application -list -appStates RUNNING | grep "$job_or_yarn_app_name" | awk '{print $1}' | while read -r appId
            do
                yarn application -kill "$appId"
                echo "Stopped YARN application with ApplicationID: $appId"
            done
            ;;
        yarn-session)
            "$FLINK_HOME"/bin/flink list -r -yid "$YARN_SESSION_CLUSTER_ID" | grep "$job_or_yarn_app_name" | awk '{print $4}' | while read -r jobId
            do
                "$FLINK_HOME"/bin/flink cancel "$jobId" -yid "$YARN_SESSION_CLUSTER_ID"
                echo "Stopped Flink job with JobID: $jobId"
            done
            ;;
        *)
            echo "Invalid DEPLOYMENT_MODE. Choose from: standalone, yarn-per-job, yarn-session"
            exit 1
            ;;
    esac
}
set -euo pipefail

# resolve links - $0 may be a softlink
PRG=$(readlink -f "$0")
PRG_DIR=$(dirname "$PRG")
APP_DIR=$(cd "$PRG_DIR/.." && pwd)
CONF_DIR="$APP_DIR/config"

# Source environment variables if available
if [ -f "${CONF_DIR}/grootstream_env.sh" ]; then
    . "${CONF_DIR}/grootstream_env.sh"
fi

# Check dependencies
if ! command -v yarn &> /dev/null || ! command -v flink &> /dev/null || [ -z "${FLINK_HOME:-}" ]; then
    echo "Flink or YARN not found or FLINK_HOME environment variable not set properly. Please check your environment setup."
    exit 1
fi

# Validate input arguments
if [ "$#" -ne 2 ]; then
    display_usage
    exit 1
fi

# Assigning input arguments to variables
deployment_mode=$1  # standalone, yarn-per-job, or yarn-session
job_or_yarn_app_name=$2  # Standalone job name or YARN application name

# Checking for empty input arguments
if [ -z "$deployment_mode" ] || [ -z "$job_or_yarn_app_name" ]; then
    display_usage
    exit 1
fi

# Execute stop_jobs function with provided arguments
stop_jobs "$deployment_mode" "$job_or_yarn_app_name"





