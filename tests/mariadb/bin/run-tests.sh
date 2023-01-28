#!/usr/bin/env bash

sleep 5

# Check migrations
flowexec -f migrations job build v1
flowexec -f migrations job build v1
flowexec -f migrations job build v2
flowexec -f migrations job build v2
flowexec -f migrations job build v1


# Run weather example
flowexec -f weather job build main --force
