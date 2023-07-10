#!/usr/bin/env bash

set -e

# Clean previous project
rm -rf quickstart-test

# Get current Flowman version
FLOWMAN_VERSION=$(mvn -f ../.. -q -N help:evaluate -Dexpression=project.version -DforceStdout)

mvn archetype:generate \
    -B \
    -DarchetypeGroupId=com.dimajix.flowman.maven \
    -DarchetypeArtifactId=flowman-archetype-assembly \
    -DarchetypeVersion=0.3.0 \
    -DgroupId=test \
    -DartifactId=quickstart-test \
    -Dversion=1.0-SNAPSHOT

# Replace Flowman version
xmlstarlet ed \
  --inplace \
  -N x=http://maven.apache.org/POM/4.0.0 \
  --update /x:project/x:parent/x:version \
  --value "$FLOWMAN_VERSION" \
  quickstart-test/pom.xml

cd quickstart-test || exit
mvn clean install

cd ..

# Clean up
rm -rf quickstart-test
