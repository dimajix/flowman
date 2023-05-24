#!/usr/bin/env bash

set -e

# Retrieve http proxy options
http_proxy_host=$(echo $http_proxy | sed 's#.*//\([^:]*\).*#\1#')
http_proxy_port=$(echo $http_proxy | sed 's#.*//[^:]*:\([0-9]*\)#\1#')
if [[ "$http_proxy_host" != "" ]]; then
    JAVA_PROXY_OPTS="-Dhttp.proxyHost=${http_proxy_host} -Dhttp.proxyPort=${http_proxy_port} $JAVA_PROXY_OPTS"
fi

https_proxy_host=$(echo $https_proxy | sed 's#.*//\([^:]*\).*#\1#')
https_proxy_port=$(echo $https_proxy | sed 's#.*//[^:]*:\([0-9]*\)#\1#')
if [[ "$https_proxy_host" != "" ]]; then
    JAVA_PROXY_OPTS="-Dhttps.proxyHost=${https_proxy_host} -Dhttps.proxyPort=${https_proxy_port} $JAVA_PROXY_OPTS"
fi

# Get current Flowman version
FLOWMAN_VERSION=$(mvn -f ../.. -q -N help:evaluate -Dexpression=project.version -DforceStdout)

# Build package
#export MAVEN_OPTS="$JAVA_PROXY_OPTS"
mvn clean install -Dflowman.version=$FLOWMAN_VERSION


# Execute tests via spark-submit
spark-submit \
    --driver-java-options "$JAVA_PROXY_OPTS" \
    target/synapse/synapse-test-1.0-SNAPSHOT.jar -f flow test run
