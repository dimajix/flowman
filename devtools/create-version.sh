#!/usr/bin/env bash

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-utils.sh"

set -e
set -o pipefail

while getopts ":v:n" opt; do
  case $opt in
    v) NEXT_VERSION=$OPTARG ;;
    n) DRY_RUN=1 ;;
    \?) error "Invalid option: $OPTARG" ;;
  esac
done

check_clean_directory
get_branch_info

git checkout master
git pull origin master
git checkout -b $GIT_BRANCH
git push origin

git checkout master
mvn versions:set -DnewVersion=$NEXT_VERSION
git commit -a -m "Set version to next master version to $NEXT_VERSION"
git push origin
