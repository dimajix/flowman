#!/usr/bin/env bash

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-utils.sh"

set -e
set -o pipefail

while getopts ":b:n" opt; do
  case $opt in
    b) GIT_BRANCH=$OPTARG ;;
    n) DRY_RUN=1 ;;
    \?) error "Invalid option: $OPTARG" ;;
  esac
done


check_clean_directory
get_release_info

git checkout $GIT_BRANCH
git pull origin $GIT_BRANCH
mvn versions:set -DnewVersion=$RELEASE_VERSION
git commit -a -m "Create release version $RELEASE_VERSION"
git tag $RELEASE_TAG
mvn versions:set -DnewVersion=$NEXT_VERSION
git commit -a -m "Set version to next SNAPSHOT $NEXT_VERSION"

git push origin
git push origin --tags
