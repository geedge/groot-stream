#!/bin/bash

set -eu

# Stop job by name in standalone mode
stop_standalone_job_by_name() {
    # Check if a Job Name is provided as an argument
    if [ $# -ne 1 ]; then
        echo "Usage: $0 <job_name>"
        exit 1
    fi

    # Assign the first argument to the variable 'job_name'
    job_name=$1

    # Get the Job ID of the job with the specified name
    job_info=$(flink list -r | grep -i "$job_name")
    job_id=$(echo "$job_info" | awk '{print $4}')

    # Check if the job with the specified name was found
    if [ -z "$job_id" ]; then
        echo "No Flink job with the name '$job_name' found to stop in standalone mode."
        exit 1
    fi

    # Stop the Flink job using the obtained Job ID
    flink cancel $job_id

    # Check the exit status of the previous command
    if [ $? -eq 0 ]; then
        echo "Flink job with Job ID $job_id and name '$job_name' has been successfully stopped in standalone mode."
        exit 0
    else
        echo "Failed to stop the Flink job with Job ID $job_id and name '$job_name' in standalone mode."
        exit 1
    fi
}

# Function to stop Flink job by name in YARN mode
stop_yarn_job_by_name() {
    # Check if a Job Name is provided as an argument
    if [ $# -ne 1 ]; then
        echo "Usage: $0 <job_name>"
        exit 1
    fi

    # Assign the first argument to the variable 'job_name'
    job_name=$1

    # Get the Flink jobs information from YARN ResourceManager
    jobs_info=$(yarn application -list -appTypes FLINK | grep -i "$job_name")

    # Extract the Application ID from the job information
    application_id=$(echo "$jobs_info" | awk '{print $1}')

    # Check if the job with the specified name was found
    if [ -z "$application_id" ]; then
        echo "No Flink job with the name '$job_name' found to stop in YARN mode."
        exit 1
    fi

    # Stop the YARN application using 'yarn application -kill' command
    yarn application -kill $application_id

    # Check the exit status of the previous command
    if [ $? -eq 0 ]; then
        echo "Flink job with Application ID $application_id and name '$job_name' has been successfully stopped in YARN mode."
        exit 0
    else
        echo "Failed to stop the Flink job with Application ID $application_id and name '$job_name' in YARN mode."
        exit 1
    fi
}

# Check if a Job Name is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <job_name>"
    exit 1
fi

# Try stopping the job by name in standalone mode first
stop_standalone_job_by_name $1 || {
    # If stopping in standalone mode fails, attempt to stop in YARN mode
    stop_yarn_job_by_name $1
}