#!/usr/bin/env bash

set -e

# Get current Flowman version
FLOWMAN_VERSION=$(mvn -f ../.. -q -N help:evaluate -Dexpression=project.version -DforceStdout)

# Build package
mvn clean install -Dflowman.version=$FLOWMAN_VERSION

# Execute tests via spark-submit
spark-submit target/synapse/synapse-test-1.0-SNAPSHOT.jar -f flow test run
