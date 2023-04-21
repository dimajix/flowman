#!/usr/bin/env bash

set -e

# Start database (in background)
docker-compose up -d mariadb

# Clean previous project
rm -rf quickstart-test

# Get current Flowman version
FLOWMAN_VERSION=$(mvn -f ../.. -q -N help:evaluate -Dexpression=project.version -DforceStdout)

# Create archetype
mvn archetype:generate \
      -DarchetypeGroupId=com.dimajix.flowman \
      -DarchetypeArtifactId=flowman-archetype-quickstart \
      -DarchetypeVersion=0.30.0 \
      -DinteractiveMode=false \
      -DgroupId=com.dimajix.flowman.integration-tests \
      -DartifactId=quickstart-test \
      -Dversion=1.0-SNAPSHOT

# Replace Flowman version
sed -i "16s#<version>.*</version>#<version>$FLOWMAN_VERSION</version>#" quickstart-test/pom.xml

# Build project
cd quickstart-test || exit
mvn clean install

# Unpack dist
tar xzf target/quickstart-test-1.0-SNAPSHOT-bin.tar.gz
cd quickstart-test-1.0-SNAPSHOT

# Run tests
unset JDBC_DRIVER=
unset JDBC_URL=
unset JDBC_DB=
unset JDBC_USERNAME=
unset JDBC_PASSWORD=
bin/flowexec -f flow job build main

# Clean up
docker-compose down
