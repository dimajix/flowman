#!/usr/bin/env bash

# Set Flowman directories
export FLOWMAN_HOME=${FLOWMAN_HOME=$(readlink -f $(dirname $0)/..)}
export FLOWMAN_CONF_DIR=${FLOWMAN_CONF_DIR=$FLOWMAN_HOME/conf}

# Load environment file if present
if [ -f $FLOWMAN_CONF_DIR/flowman-env.sh ]; then
    source $FLOWMAN_CONF_DIR/flowman-env.sh
fi

if [ -f $HADOOP_HOME/etc/hadoop/hadoop-env.sh ]; then
    source $HADOOP_HOME/etc/hadoop/hadoop-env.sh
fi

# Set basic Spark options
: ${SPARK_MASTER:="yarn"}
: ${SPARK_EXECUTOR_CORES:="4"}
: ${SPARK_EXECUTOR_MEMORY:="8G"}
: ${SPARK_DRIVER_MEMORY:="2G"}

: ${SPARK_SUBMIT:=$SPARK_HOME/bin/spark-submit}
: ${SPARK_OPTS:=""}
: ${SPARK_DRIVER_JAVA_OPTS:="-server"}
: ${SPARK_EXECUTOR_JAVA_OPTS:="-server"}

# Build Spark dist classpath
if [ "$SPARK_DIST_CLASSPATH" = "" ]; then
    if [ -d "$HADOOP_HOME" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$HADOOP_HOME/*.jar:$HADOOP_HOME/lib/*.jar"
    fi
    if [ -d "$HADOOP_CONF_DIR" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$HADOOP_CONF_DIR/*"
    fi
    if [ -d "$HADOOP_HOME/share/hadoop/common" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$HADOOP_HOME/share/hadoop/common/*.jar:$HADOOP_HOME/share/hadoop/common/lib/*.jar"
    fi

    if [ -d "$YARN_HOME" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$YARN_HOME/*.jar:$YARN_HOME/lib/*.jar"
    elif [ -d "$HADOOP_HOME/share/hadoop/yarn" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$HADOOP_HOME/share/hadoop/yarn/*.jar:$HADOOP_HOME/share/hadoop/yarn/lib/*.jar"
    fi

    if [ -d "$HDFS_HOME" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$HDFS_HOME/*.jar:$HDFS_HOME/lib/*.jar"
    elif [ -d "$HADOOP_HOME/share/hadoop/hdfs" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$HADOOP_HOME/share/hadoop/hdfs/*.jar:$HADOOP_HOME/share/hadoop/hdfs/lib/*.jar"
    fi

    if [ -d "$MAPRED_HOME" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$MAPRED_HOME/*:$MAPRED_HOME/*.jar"
    fi
fi


# Add Kerberos authentication
if [ "$KRB_PRINCIPAL" != "" ]; then
    SPARK_OPTS="--principal $KRB_PRINCIPAL --keytab $KRB_KEYTAB $SPARK_OPTS"
fi

# Add YARN ququq
if [ "$YARN_QUEUE" != "" ]; then
    SPARK_OPTS="--queue $YARN_QUEUE $SPARK_OPTS"
fi


spark_submit() {
    LIB_JARS=$(ls $FLOWMAN_HOME/lib/*.jar | awk -vORS=, '{ print $1 }' | sed 's/,$/\n/')

    $SPARK_SUBMIT \
      --executor-cores $SPARK_EXECUTOR_CORES \
      --executor-memory $SPARK_EXECUTOR_MEMORY \
      --driver-memory $SPARK_DRIVER_MEMORY \
      --driver-java-options "$SPARK_DRIVER_JAVA_OPTS" \
      --conf spark.executor.extraJavaOptions="$SPARK_EXECUTOR_JAVA_OPTS" \
      --master $SPARK_MASTER \
      --class $2 \
      $SPARK_OPTS \
      --jars $LIB_JARS \
      $1 "${@:3}"
}
