#!/usr/bin/env bash
set -eo pipefail

export FLOWMAN_HOME=${FLOWMAN_HOME=/opt/flowman}
export FLOWMAN_CONF_DIR=${FLOWMAN_CONF_DIR=$FLOWMAN_HOME/conf}

export FLOWMAN_LOGDB_DRIVER=${FLOWMAN_LOG_DRIVER="org.apache.derby.jdbc.EmbeddedDriver"}
export FLOWMAN_LOGDB_URL=${FLOWMAN_LOG_DRIVER="jdbc:derby:/opt/flowman/logdb;create=true"}
export FLOWMAN_LOGDB_USER=${FLOWMAN_LOGDB_USER=""}
export FLOWMAN_LOGDB_PASSWORD=${FLOWMAN_LOGDB_PASSWORD=""}
