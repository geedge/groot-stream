#!/bin/bash

set -eu
# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ] ; do
  # shellcheck disable=SC2006
  ls=`ls -ld "$PRG"`
  # shellcheck disable=SC2006
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    # shellcheck disable=SC2006
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRG_DIR=`dirname "$PRG"`
# shellcheck disable=SC2006
APP_DIR=`cd "$PRG_DIR/.." >/dev/null; pwd`
CONF_DIR=${APP_DIR}/config
APP_JAR=${APP_DIR}/bootstrap/groot-bootstrap.jar
APP_MAIN="com.geedgenetworks.bootstrap.main.GrootStreamRunner"

if [ -f "${CONF_DIR}/grootstream_env.sh" ]; then
    . "${CONF_DIR}/grootstream_env.sh"
fi

if [ $# == 0 ]
then
    args="-h"
else
    args=$@
fi

set +u

if [ -z $HAZELCAST_CONFIG ]; then
  HAZELCAST_CONFIG=${CONF_DIR}/hazelcast.yaml
fi

if [ -z $GROOTSTREAM_CONFIG ]; then
    GROOTSTREAM_CONFIG=${CONF_DIR}/grootstream.yaml
fi

if [ -z $UDF_CONFIG ]; then
    UDF_CONFIG=${CONF_DIR}/udf.plugins
fi

args="${args:-} -i grootstream.config=${GROOTSTREAM_CONFIG}"
args="${args:-} -i hazelcast.config=${HAZELCAST_CONFIG}"
args="${args:-} -i udf.config=${UDF_CONFIG}"

# Log4j2 Config
if [ -e "${CONF_DIR}/log4j2.properties" ]; then
  JAVA_OPTS="${JAVA_OPTS:-} -Dlog4j2.configurationFile=${CONF_DIR}/log4j2.properties"
  JAVA_OPTS="${JAVA_OPTS:-} -Dgrootstream.logs.path=${APP_DIR}/logs"
  JAVA_OPTS="${JAVA_OPTS:-} -Dgrootstream.logs.file_name=groot-stream-runner"
fi


CLASS_PATH=${APP_DIR}/bootstrap/logging/*:${APP_JAR}

CMD=$(java ${JAVA_OPTS} -cp ${CLASS_PATH} ${APP_MAIN} ${args}) && EXIT_CODE=$? || EXIT_CODE=$?

if [ ${EXIT_CODE} -eq 2618 ]; then
    # print usage
    echo "${CMD}"
    exit 0
elif [ ${EXIT_CODE} -eq 0 ]; then
    echo "Execute GrootStream Job: $(echo "${CMD}" | tail -n 1)"
    eval $(echo "${CMD}" | tail -n 1)
else
    echo "${CMD}"
    exit ${EXIT_CODE}
fi
