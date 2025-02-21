#!/bin/bash

set -e

if [[ -z "$1" ]]
then
  echo "Please specify the release tag"
  exit 1
fi

rm -rf tmp
mkdir tmp

git clone https://github.com/googleapis/dotnet-spanner-entity-framework.git \
  --depth 1 -b $1 --recursive tmp/release

cd tmp/release
./build.sh
