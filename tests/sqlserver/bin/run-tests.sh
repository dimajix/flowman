#!/usr/bin/env bash

set -e

# Check migrations
flowexec -f migrations job build v1
flowexec -f migrations job build v1
flowexec -f migrations job build v2
flowexec -f migrations job build v2
flowexec -f migrations job build v1

flowexec -f migrations job build test_timestamp


# Run weather example
flowexec -f weather job build main --force
