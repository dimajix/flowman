#!/usr/bin/env bash

set -e

mvn archetype:generate \
    -B \
    -DarchetypeGroupId=com.dimajix.flowman.maven \
    -DarchetypeArtifactId=flowman-archetype-assembly \
    -DgroupId=test \
    -DartifactId=quickstart \
    -Dversion=1.0-SNAPSHOT \
    -DflowmanVersion=1.0.0-SNAPSHOT

cd quickstart || exit
mvn clean install

cd ..
rm -rf quickstart
