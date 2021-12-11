#!/usr/bin/env bash

# Apply any proxy settings
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

# Allocate somewhat more memory for driver
SPARK_DRIVER_MEMORY="4G"
