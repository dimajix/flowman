#!/usr/bin/env bash

# Set Flowman directories
export FLOWMAN_HOME=${FLOWMAN_HOME=$(readlink -f $(dirname $0)/..)}
export FLOWMAN_CONF_DIR=${FLOWMAN_CONF_DIR=$FLOWMAN_HOME/conf}

# Load environment file if present
if [ -f "$FLOWMAN_CONF_DIR/flowman-env.sh" ]; then
    source "$FLOWMAN_CONF_DIR/flowman-env.sh"
fi

if [ -f "$HADOOP_HOME/etc/hadoop/hadoop-env.sh" ]; then
    source "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"
fi

# Add log4j config
if [ -f "$FLOWMAN_CONF_DIR/log4j.properties" ]; then
    SPARK_DRIVER_LOGGING_OPTS="-Dlog4j.configuration=$FLOWMAN_CONF_DIR/log4j.properties"
fi
if [ -f "$FLOWMAN_CONF_DIR/log4j2.properties" ]; then
    SPARK_DRIVER_LOGGING_OPTS="-Dlog4j.configurationFile=$FLOWMAN_CONF_DIR/log4j2.properties"
fi


# Set basic Spark options
: ${SPARK_SUBMIT:="$SPARK_HOME"/bin/spark-submit}
: ${SPARK_OPTS:=""}
: ${SPARK_JARS:=""}
: ${SPARK_DRIVER_JAVA_OPTS:=""}
: ${SPARK_EXECUTOR_JAVA_OPTS:=""}


# Build Spark dist classpath
if [ "$SPARK_DIST_CLASSPATH" = "" ]; then
    if [ -d "$HADOOP_HOME" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:"$HADOOP_HOME"/*.jar:$HADOOP_HOME/lib/*.jar"
    fi
    if [ -d "$HADOOP_CONF_DIR" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:"$HADOOP_CONF_DIR"/*"
    fi
    if [ -d "$HADOOP_HOME/share/hadoop/common" ]; then
        export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:"$HADOOP_HOME"/share/hadoop/common/*.jar:$HADOOP_HOME/share/hadoop/common/lib/*.jar"
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


# Add Optional settings to SPARK_OPTS
if [ "$KRB_PRINCIPAL" != "" ]; then
    SPARK_OPTS="--principal $KRB_PRINCIPAL --keytab $KRB_KEYTAB $SPARK_OPTS"
fi
if [ "$YARN_QUEUE" != "" ]; then
    SPARK_OPTS="--queue $YARN_QUEUE $SPARK_OPTS"
fi
if [ "$SPARK_MASTER" != "" ]; then
    SPARK_OPTS="--master $SPARK_MASTER $SPARK_OPTS"
fi
if [ "$SPARK_EXECUTOR_CORES" != "" ]; then
    SPARK_OPTS="--executor-cores $SPARK_EXECUTOR_CORES $SPARK_OPTS"
fi
if [ "$SPARK_EXECUTOR_MEMORY" != "" ]; then
    SPARK_OPTS="--executor-memory $SPARK_EXECUTOR_MEMORY $SPARK_OPTS"
fi
if [ "$SPARK_DRIVER_CORES" != "" ]; then
    SPARK_OPTS="--driver-cores $SPARK_DRIVER_CORES $SPARK_OPTS"
fi
if [ "$SPARK_DRIVER_MEMORY" != "" ]; then
    SPARK_OPTS="--driver-memory $SPARK_DRIVER_MEMORY $SPARK_OPTS"
fi



flowman_spark_lib() {
	echo $1 | awk -F, '{for(i=1;i<=NF;i++) printf("%s%s",ENVIRON["FLOWMAN_HOME"]"/"$i,(i<NF)?",":"")}'
}

flowman_java_lib() {
	echo $1 | awk -F, '{for(i=1;i<=NF;i++) printf("%s%s",ENVIRON["FLOWMAN_HOME"]"/"$i,(i<NF)?":":"")}'
}


run_spark() {
    if [ "$SPARK_JARS" != "" ]; then
        extra_jars=",$SPARK_JARS"
    else
        extra_jars=""
    fi

    exec $SPARK_SUBMIT \
      --driver-java-options "$SPARK_DRIVER_LOGGING_OPTS $SPARK_DRIVER_JAVA_OPTS" \
      --conf spark.executor.extraJavaOptions="$SPARK_EXECUTOR_JAVA_OPTS" \
      --class "$3" \
      $SPARK_OPTS \
      --jars "$(flowman_spark_lib $2)$extra_jars" \
      $FLOWMAN_HOME/lib/$1 "${@:4}"
}


run_java() {
    exec java \
     -cp "$(flowman_java_lib $2):$FLOWMAN_HOME/lib/$1" \
      "$3" \
     "${@:4}"
}
