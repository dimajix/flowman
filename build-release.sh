#!/usr/bin/env bash

set -eo pipefail


FLOWMAN_VERSION=$(mvn -q -N help:evaluate -Dexpression=project.version -DforceStdout)
echo "Building Flowman release version ${FLOWMAN_VERSION}"

mkdir -p release


build_profile() {
    profiles=$@

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
build_profile -phadoop-2.6 -pspark-2.4
build_profile -phadoop-2.7 -pspark-2.4

export JAVA_HOME=
build_profile -phadoop-2.7 -pspark-3.0
build_profile -phadoop-3.2 -pspark-3.0
build_profile -phadoop-2.7 -pspark-3.1
build_profile -phadoop-3.2 -pspark-3.1
build_profile -phadoop-2.7 -pspark-3.2
build_profile -phadoop-3.3 -pspark-3.2 -Dhadoop.version=3.3.1
build_profile -phadoop-2.7 -pspark-3.3
build_profile -phadoop-3.3 -pspark-3.3 -Dhadoop.version=3.3.2

export JAVA_HOME=/usr/lib/jvm/java-1.8.0
build_profile -pCDH-6.3
build_profile -pCDP-7.1

# Finally build default version
export JAVA_HOME=
mvn clean deploy -DskipTests
cp flowman-dist/target/flowman-dist-*.tar.gz release
