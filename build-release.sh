#!/usr/bin/env bash

set +e
set +o pipefail


FLOWMAN_VERSION=$(mvn -q -N help:evaluate -Dexpression=project.version -DforceStdout)
echo "Building Flowman release version ${FLOWMAN_VERSION}"

mkdir -p release


build_profile() {
    profiles=""
    for p in $@
    do
        profiles="$profiles -P$p"
    done

    # Set new version
    HADOOP_DIST=$(mvn $profiles -q -N help:evaluate -Dexpression=hadoop.dist -DforceStdout)
    SPARK_API_VERSION=$(mvn $profiles -q -N help:evaluate -Dexpression=spark-api.version -DforceStdout)
    HADOOP_API_VERSION=$(mvn $profiles -q -N help:evaluate -Dexpression=hadoop-api.version -DforceStdout)

    echo "Building for dist $HADOOP_DIST with Spark $SPARK_API_VERSION and Hadoop $HADOOP_API_VERSION"
    mvn -q versions:set -DnewVersion=${FLOWMAN_VERSION}-${HADOOP_DIST}-spark${SPARK_API_VERSION}-hadoop${HADOOP_API_VERSION}

    mvn clean deploy $profiles -DskipTests -Dflowman.dist.suffix="" -Ddockerfile.skip
    #mvn clean install $profiles -DskipTests -Dflowman.dist.suffix="" -Ddockerfile.skip

    cp flowman-dist/target/flowman-dist-*.tar.gz release

    # Revert to original version
    mvn -q versions:revert
}


export JAVA_HOME=/usr/lib/jvm/java-1.8.0
build_profile hadoop-2.6 spark-2.4
build_profile hadoop-2.7 spark-2.4

export JAVA_HOME=
build_profile hadoop-2.7 spark-3.0
build_profile hadoop-3.2 spark-3.0
build_profile hadoop-2.7 spark-3.1
build_profile hadoop-3.2 spark-3.1
build_profile hadoop-2.7 spark-3.2
build_profile hadoop-3.3 spark-3.2

export JAVA_HOME=/usr/lib/jvm/java-1.8.0
build_profile CDH-6.3
build_profile CDP-7.1

# Finally build default version
mvn clean install -DskipTests
