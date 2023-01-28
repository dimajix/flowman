#!/usr/bin/env bash

# Make projects world readable
find . -type f | xargs chmod a+r
find . -type d | xargs chmod a+rx
find bin -type f | xargs chmod a+rx

# Start database
docker-compose up -d mariadb

# Run tests
docker-compose run --rm flowman bin/run-tests.sh

# Clean up
docker-compose down
