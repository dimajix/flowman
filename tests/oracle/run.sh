#!/usr/bin/env bash

set -e

# Make projects world readable
find . -type f | xargs chmod a+r
find . -type d | xargs chmod a+rx
find bin -type f | xargs chmod a+rx

# Start database
docker-compose up -d oracle

# Run tests
sleep 10
docker-compose run --rm flowman bin/run-tests.sh

# Clean up
docker-compose down
