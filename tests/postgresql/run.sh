#!/usr/bin/env bash

set -e

# Get current Flowman version
FLOWMAN_TAG=$(mvn -f ../.. -q -N help:evaluate -Dexpression=flowman.dist.label -DforceStdout)
docker image tag dimajix/flowman:$FLOWMAN_TAG flowman-it-postgresql:latest

# Make projects world readable
find . -type f | xargs chmod a+r
find . -type d | xargs chmod a+rx
find bin -type f | xargs chmod a+rx

# Start database
docker-compose up -d postgresql

# Run tests
sleep 10
docker-compose run --rm flowman bin/run-tests.sh

# Clean up
docker-compose down
docker image rm flowman-it-postgresql:latest
