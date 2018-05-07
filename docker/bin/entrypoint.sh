#!/usr/bin/env bash
set -eo pipefail

# Setup environment
source /opt/docker/libexec/flowman-init.sh

exec "$@"
