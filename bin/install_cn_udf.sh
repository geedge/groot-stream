#!/bin/bash
# This script used to register the UDF functions required during the running process.
# All are registered by default. You can also choose what you need. At same time, you need to configure knowledge base in config/grootstream.yaml.

# get groot stream home

GROOTSTREAM_HOME=$(cd $(dirname $0);cd ../;pwd)

#Default version is 1.3.1, you can also choose a custom version. eg: 1.4.0:  sh install_cn_udf.sh 1.4.0
VERSION_DEFAULT=1.3.1

CN_UDF_FILE="${GROOTSTREAM_HOME}/plugins/cn_udf.plugins"

GROOTSTREAM_UDF_FILE="${GROOTSTREAM_HOME}/config/udf.plugins"

# Function to log messages
log() {
    echo "$(date +"%Y-%m-%d %H:%M:%S") - $1"
}

# Function to handle errors
handle_error() {
    local message=$1
    log "ERROR: $message"
    exit 1
}

# Validate arguments
if [ $# -gt 1 ]; then
    handle_error "Usage: $0 [version]"
fi

# Set version
version=${1:-$VERSION_DEFAULT}
log "Installing CN UDFs. Version: $version"


# Verify existence of necessary files
if [ ! -f  "$CN_UDF_FILE" ]; then
    handle_error "File $CN_UDF_FILE not found."
fi

# Ensure GROOTSTREAM_UDF_FILE ends with a blank line
if [ -n "$(tail -c 1 "$GROOTSTREAM_UDF_FILE")" ]; then
    echo "" >> "$GROOTSTREAM_UDF_FILE"
fi

# Register UDFs
while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" == com.geedgenetworks.core.udf* ]] && ! grep -qxF "$line" "$GROOTSTREAM_UDF_FILE"; then
        log "Registering UDF: $line"
        echo "$line" >> "$GROOTSTREAM_UDF_FILE" || handle_error "Failed to register UDF: $line"
    fi
done < "$CN_UDF_FILE"

# shellcheck disable=SC1073
log "Installation completed successfully."