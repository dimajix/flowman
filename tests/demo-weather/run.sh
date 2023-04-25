#!/usr/bin/env bash

set -e

# Clean previous project
rm -rf demo-weather

# Get current Flowman version
FLOWMAN_VERSION=$(mvn -f ../.. -q -N help:evaluate -Dexpression=project.version -DforceStdout)
FLOWMAN_TAG=$(mvn -f ../.. -q -N help:evaluate -Dexpression=flowman.dist.label -DforceStdout)

# Clone Flowman Tutorial
rm -rf demo-weather
git clone https://github.com/dimajix/flowman-demo-weather demo-weather

# Replace Flowman version
xmlstarlet ed \
  --inplace \
  -N x=http://maven.apache.org/POM/4.0.0 \
  --update /x:project/x:parent/x:version \
  --value "$FLOWMAN_VERSION" \
  demo-weather/pom.xml

cd demo-weather || exit
mvn clean install

# Make projects world readable, such that the directory is readable as a volume mounted into Docker
chmod -R a+r .
find . -type d | xargs chmod a+rx

# Start environment
docker-compose up -d mariadb prometheus pushgateway
sleep 10
docker-compose up -d flowman-server

# Run Flowman
docker-compose run --rm flowman flowexec -f flow job build main --force

# Clean up
docker-compose down
cd ..

# Clean up
rm -rf demo-weather
