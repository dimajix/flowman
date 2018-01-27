#!/usr/bin/env bash

basedir=$(readlink -f $(dirname $0/..))
confdir=$basedir/conf

# Set basic options
: ${SPARK_MASTER:="yarn"}
: ${SPARK_EXECUTOR_CORES:="4"}
: ${SPARK_EXECUTOR_MEMORY:="8G"}
: ${SPARK_DRIVER_MEMORY:="2G"}

: ${SPARK_OPTS:=""}
: ${SPARK_DRIVER_JAVA_OPTS:="-server"}
: ${SPARK_EXECUTOR_JAVA_OPTS:="-server"}

# Load environment file if present
if [ -f $confdir/dataflow-env.sh ]; then
    source $confdir/dataflow-env.sh
fi

# Build Spark dist classpath
export SPARK_DIST_CLASSPATH=""
if [ -d $HADOOP_HOME ]; then
    export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$HADOOP_HOME/*.jar:$HADOOP_HOME/lib/*.jar"
fi
if [ -d $HADOOP_CONF_DIR ]; then
    export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$HADOOP_CONF_DIR/*"
fi
if [ -d $YARN_HOME ]; then
    export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$YARN_HOME/*.jar:$YARN_HOME/lib/*.jar"
fi
if [ -d $HDFS_HOME ]; then
    export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$HDFS_HOME/*.jar:$HDFS_HOME/lib/*.jar"
fi
if [ -d $MAPRED_HOME ]; then
    export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$MAPRED_HOME/*:$MAPRED_HOME/*.jar"
fi


spark_submit() {
    LIB_JARS=$(ls $basedir/lib/*.jar | awk -vORS=, '{ print $1 }' | sed 's/,$/\n/')

    $SPARK_HOME/bin/spark-submit \
      --executor-cores $SPARK_EXECUTOR_CORES \
      --executor-memory $SPARK_EXECUTOR_MEMORY \
      --driver-memory $SPARK_DRIVER_MEMORY \
      --driver-java-options "$SPARK_DRIVER_JAVA_OPTS" \
      --conf spark.executor.extraJavaOptions="$SPARK_EXECUTOR_JAVA_OPTS" \
      --master $SPARK_MASTER \
      --class $1 \
      $SPARK_OPTS \
      --jars $LIB_JARS \
      $2 "${@:3}"
}
