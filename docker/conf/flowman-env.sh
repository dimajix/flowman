#!/usr/bin/env bash

# Apply any proxy settings
if [[ $PROXY_HOST != "" ]]; then
    SPARK_DRIVER_JAVA_OPTS="
        -Dhttp.proxyHost=${PROXY_HOST}
        -Dhttp.proxyPort=${PROXY_PORT}
        -Dhttps.proxyHost=${PROXY_HOST}
        -Dhttps.proxyPort=${PROXY_PORT}
        $SPARK_DRIVER_JAVA_OPTS"

    SPARK_EXECUTOR_JAVA_OPTS="
        -Dhttp.proxyHost=${PROXY_HOST}
        -Dhttp.proxyPort=${PROXY_PORT}
        -Dhttps.proxyHost=${PROXY_HOST}
        -Dhttps.proxyPort=${PROXY_PORT}
        $SPARK_EXECUTOR_JAVA_OPTS"

    SPARK_OPTS="
        --conf spark.hadoop.fs.s3a.proxy.host=${PROXY_HOST}
        --conf spark.hadoop.fs.s3a.proxy.port=${PROXY_PORT}
        $SPARK_OPTS"
fi
