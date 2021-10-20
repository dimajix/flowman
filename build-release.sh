#!/usr/bin/env bash

rm -rf release
mkdir release

build_profile() {
    profiles=""
    for p in $@
    do
        profiles="$profiles -P$p"
    done
    mvn clean install $profiles -DskipTests -Ddockerfile.skip
    cp flowman-dist/target/flowman-dist-*.tar.gz release
}

build_profile hadoop-2.6 spark-2.4
build_profile hadoop-2.7 spark-2.4
build_profile hadoop-2.7 spark-3.0
build_profile hadoop-3.2 spark-3.0
build_profile hadoop-2.7 spark-3.1
build_profile hadoop-3.2 spark-3.1
build_profile hadoop-2.7 spark-3.2
build_profile hadoop-3.3 spark-3.2
build_profile CDH-6.3
build_profile CDP-7.1
