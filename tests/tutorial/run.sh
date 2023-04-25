#!/usr/bin/env bash

set -e

# Get current Flowman version
FLOWMAN_TAG=$(mvn -f ../.. -q -N help:evaluate -Dexpression=flowman.dist.label -DforceStdout)

# Clone Flowman Tutorial
rm -rf tutorial
git clone https://github.com/dimajix/flowman-tutorial.git tutorial

# Make projects world readable, such that the directory is readable as a volume mounted into Docker
chmod -R a+r tutorial
find tutorial -type d | xargs chmod a+rx

# Enter Tutorials directory
cd tutorial || exit

# Fix Flowman version
sed -i "s#dimajix/flowman:.*#dimajix/flowman:$FLOWMAN_TAG#" docker-compose.yml

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
