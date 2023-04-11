#!/usr/bin/env bash

FLOWMAN_REPO="git@github.com:dimajix/flowman"


function error {
  echo "$*"
  exit 1
}

function read_config {
  local PROMPT="$1"
  local DEFAULT="$2"
  local REPLY=

  read -p "$PROMPT [$DEFAULT]: " REPLY
  local RETVAL="${REPLY:-$DEFAULT}"
  if [ -z "$RETVAL" ]; then
    error "$PROMPT is must be provided."
  fi
  echo "$RETVAL"
}

function parse_version {
  grep -e '<version>.*</version>' | \
    head -n 1 | tail -n 1 | cut -d'>' -f2 | cut -d '<' -f1
}

function check_for_tag {
  git ls-remote --tags "$FLOWMAN_REPO" "$1" | grep "$1" > /dev/null
}

function get_git_config {
  GIT_NAME=$(git config user.name)
  GIT_EMAIL=$(git config user.email)

  export GIT_NAME
  export GIT_EMAIL
}

function check_clean_directory {
  result=$(git status -uno -s)
  if [ -n "$result" ]
  then
    error "Directory is not clean"
  fi
}

function check_version {
  local VERSION=$1
  if [[ ! $VERSION =~ ([0-9]+)\.([0-9]+)\.[0-9]+-SNAPSHOT ]]; then
    error "Not a SNAPSHOT version: $VERSION"
  fi
}


function get_branch_info {
  # Find the current version for the branch.
  local VERSION=$(cat $SELF/../pom.xml | parse_version)
  echo "Current branch version is $VERSION"

  check_version "$VERSION"

  NEXT_VERSION="$VERSION"
  local MAJOR_VERSION=$(echo "$VERSION" | cut -d . -f 1)
  local MINOR_VERSION=$(echo "$VERSION" | cut -d . -f 2)
  GIT_BRANCH="branch-${MAJOR_VERSION}.${MINOR_VERSION}"

  MINOR_VERSION=$((MINOR_VERSION + 1))
  NEXT_VERSION="${MAJOR_VERSION}.${MINOR_VERSION}.0-SNAPSHOT"
  NEXT_VERSION=$(read_config "Next version" "$NEXT_VERSION")

  get_git_config

  export GIT_BRANCH
  export NEXT_VERSION

  cat <<EOF
================
Branching details:
BRANCH NAME:     $GIT_BRANCH
BRANCH VERSION:  $VERSION
MAIN VERSION:    $NEXT_VERSION

FULL NAME:  $GIT_NAME
E-MAIL:     $GIT_EMAIL
================
EOF

  read -p "Is this info correct [y/n]? " ANSWER
  if [ "$ANSWER" != "y" ]; then
    echo "Exiting."
    exit 1
  fi
}


function get_release_info {
  if [ -z "$GIT_BRANCH" ]; then
    # If no branch is specified, found out the latest branch from the repo.
    GIT_BRANCH=$(git ls-remote --heads "$FLOWMAN_REPO" |
      grep -v refs/heads/main |
      grep branch |
      awk '{print $2}' |
      sort -r |
      head -n 1 |
      cut -d/ -f3)
  fi

  GIT_BRANCH=$(read_config "Branch" "$GIT_BRANCH")

  # Find the current version for the branch.
  local VERSION=$(cat $SELF/../pom.xml | parse_version)
  echo "Current branch version is $VERSION."

  check_version "$VERSION"

  NEXT_VERSION="$VERSION"
  RELEASE_VERSION="${VERSION/-SNAPSHOT/}"
  SHORT_VERSION=$(echo "$VERSION" | cut -d . -f 1-2)
  local REV=$(echo "$RELEASE_VERSION" | cut -d . -f 3)

  REV=$((REV + 1))
  NEXT_VERSION="${SHORT_VERSION}.${REV}-SNAPSHOT"

  # Check if the RC already exists, and if re-creating the RC, skip tag creation.
  RELEASE_TAG="${RELEASE_VERSION}"
  SKIP_TAG=0
  if check_for_tag "$RELEASE_TAG"; then
    read -p "$RELEASE_TAG already exists. Continue anyway [y/n]? " ANSWER
    if [ "$ANSWER" != "y" ]; then
      error "Exiting."
    fi
    SKIP_TAG=1
  fi

  export GIT_BRANCH
  export NEXT_VERSION
  export RELEASE_VERSION
  export RELEASE_TAG

  get_git_config

  cat <<EOF
================
Release details:
BRANCH:     $GIT_BRANCH
TAG:        $RELEASE_TAG
RELEASE:    $RELEASE_VERSION
NEXT:       $NEXT_VERSION

FULL NAME:  $GIT_NAME
E-MAIL:     $GIT_EMAIL
================
EOF

  read -p "Is this info correct [y/n]? " ANSWER
  if [ "$ANSWER" != "y" ]; then
    echo "Exiting."
    exit 1
  fi
}


function is_dry_run {
  [[ $DRY_RUN = 1 ]]
}
