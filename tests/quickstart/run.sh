#!/usr/bin/env bash

# Start database (in background)
docker-compose up -d mariadb

# Clean previous project
rm -rf quickstart-test

# Create archetype
mvn archetype:generate \
      -DarchetypeGroupId=com.dimajix.flowman \
      -DarchetypeArtifactId=flowman-archetype-quickstart \
      -DarchetypeVersion=1.0.0-SNAPSHOT \
      -DinteractiveMode=false \
      -DgroupId=com.dimajix.flowman.integration-tests \
      -DartifactId=quickstart-test \
      -Dversion=1.0-SNAPSHOT

# Build project
cd quickstart-test
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
