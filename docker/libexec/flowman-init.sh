#!/usr/bin/env bash
set -eo pipefail

# Setup environment
source /opt/docker/libexec/spark-init.sh
source /opt/docker/libexec/flowman-vars.sh

render_templates /opt/docker/conf/flowman ${FLOWMAN_CONF_DIR}
