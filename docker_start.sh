#! /usr/bin/env bash
set -Eeuxo pipefail;

docker build . --tag starfish-integration-tests:0.1

! docker rm -f starfish-tests

docker run --net="host" \
  --name starfish-tests \
  starfish-integration-tests:0.1

