#!/usr/bin/env bash

# Apply any proxy settings from the system environment
#
http_proxy_host=$(echo $http_proxy | sed 's#.*//\([^:]*\).*#\1#') && \
http_proxy_port=$(echo $http_proxy | sed 's#.*//[^:]*:\([0-9]*\)#\1#') && \
if [[ "$http_proxy_host" != "" ]]; then
    SPARK_DRIVER_JAVA_OPTS="-Dhttp.proxyHost=${http_proxy_host} -Dhttp.proxyPort=${http_proxy_port} $SPARK_DRIVER_JAVA_OPTS"
    SPARK_EXECUTOR_JAVA_OPTS="-Dhttp.proxyHost=${http_proxy_host} -Dhttp.proxyPort=${http_proxy_port} $SPARK_EXECUTOR_JAVA_OPTS"
    SPARK_OPTS="--conf spark.hadoop.fs.s3a.proxy.host=${http_proxy_host} --conf spark.hadoop.fs.s3a.proxy.port=${http_proxy_port} $SPARK_OPTS"
fi

https_proxy_host=$(echo $https_proxy | sed 's#.*//\([^:]*\).*#\1#') && \
https_proxy_port=$(echo $https_proxy | sed 's#.*//[^:]*:\([0-9]*\)#\1#') && \
if [[ "$https_proxy_host" != "" ]]; then
    SPARK_DRIVER_JAVA_OPTS="-Dhttps.proxyHost=${http_proxy_host} -Dhttps.proxyPort=${http_proxy_port} $SPARK_DRIVER_JAVA_OPTS"
    SPARK_EXECUTOR_JAVA_OPTS="-Dhttps.proxyHost=${http_proxy_host} -Dhttps.proxyPort=${http_proxy_port} $SPARK_EXECUTOR_JAVA_OPTS"
fi


# Set AWS credentials if required. You can also specify these in project config
#
if [[ "$AWS_ACCESS_KEY_ID" != "" ]]; then
    SPARK_OPTS="
        --conf spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}
        --conf spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}
        $SPARK_OPTS"
fi

# set pod IP to work around DNS issue
if [[ $SPARK_DRIVER_HOST != "" ]]; then
    SPARK_OPTS="
        --conf spark.driver.host=${SPARK_DRIVER_HOST}
    $SPARK_OPTS"
fi

# Set extra options for local development
if [[ $SPARK_DRIVER_PORT != "" ]]; then
    SPARK_OPTS="
        --conf spark.driver.port=${SPARK_DRIVER_PORT:=19099}
        --conf spark.driver.blockManager.port=${SPARK_BLOCKMANAGER_PORT:=19098}
        --conf spark.driver.bindAddress=${BIND_ADDRESS:=0.0.0.0}
        --conf spark.network.timeout=${SPARK_NETWORK_TIMEOUT:=900s}
        --conf spark.sql.broadcastTimeout=${SPARK_BROADCAST_TIMEOUT:=900s}
        $SPARK_OPTS"
fi

# Allocate somewhat more memory for driver
SPARK_DRIVER_MEMORY="4G"
