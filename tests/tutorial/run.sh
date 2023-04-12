#!/usr/bin/env bash

set -e

# Clone Flowman Tutorial
rm -rf tutorial
git clone https://github.com/dimajix/flowman-tutorial.git tutorial
# Fix permissions
chmod -R a+r tutorial
find tutorial -type d | xargs chmod a+rx

# Enter Tutorials directory
cd tutorial || exit

# Start SQL Server
docker-compose up -d sqlserver
sleep 6


# Run tutorials
docker-compose run --rm flowman flowexec -f lessons/01-basics job build main --force
docker-compose run --rm flowman flowexec -f lessons/02-schema job build main --force
docker-compose run --rm flowman flowexec -f lessons/03-transformations job build main --force
docker-compose run --rm flowman flowexec -f lessons/04-parameters job build main year=2014 --force


# Clean up
docker-compose down

cd ..
rm -rf tutorial
