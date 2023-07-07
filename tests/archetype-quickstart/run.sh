#!/usr/bin/env bash

set -e

# Clean previous project
rm -rf quickstart-test

# Get current Flowman version
FLOWMAN_VERSION=$(mvn -f ../.. -q -N help:evaluate -Dexpression=project.version -DforceStdout)

mvn archetype:generate \
    -B \
    -DarchetypeGroupId=com.dimajix.flowman.maven \
    -DarchetypeArtifactId=flowman-archetype-quickstart \
    -DarchetypeVersion=0.2.1 \
    -DgroupId=test \
    -DartifactId=quickstart-test \
    -Dversion=1.0-SNAPSHOT \
    -DflowmanVersion=$FLOWMAN_VERSION

cd quickstart-test || exit
mvn clean install

cd ..

# Clean up
rm -rf quickstart-test
