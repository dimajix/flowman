#!/usr/bin/env bash

rm -rf release
mkdir release

build_profile() {
    profiles=""
    for p in $@
    do
        profiles="$profiles -P$p"
    done
    mvn clean install $profiles -DskipTests
    cp flowman-dist/target/flowman-dist-*.tar.gz release
}

build_profile hadoop-2.6 spark-2.3
build_profile hadoop-2.6 spark-2.4
build_profile hadoop-2.7 spark-2.3
build_profile hadoop-2.7 spark-2.4
build_profile hadoop-2.8 spark-2.3
build_profile hadoop-2.8 spark-2.4
build_profile hadoop-2.9 spark-2.3
build_profile hadoop-2.9 spark-2.4
build_profile hadoop-2.9 spark-3.0
build_profile hadoop-3.1 spark-3.0
build_profile hadoop-3.2 spark-3.0
build_profile hadoop-3.2 spark-3.1
build_profile CDH-5.15
build_profile CDH-6.3
