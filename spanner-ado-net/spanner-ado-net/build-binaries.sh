#!/bin/bash

SKIP_CLONE=$1
source spanner-lib-version.sh

if ! [ "$SKIP_CLONE" == "true" ]; then
  git clone git@github.com:googleapis/go-sql-spanner.git --branch "$SPANNER_LIB_BRANCH"
else
  cd go-sql-spanner
  git checkout "$SPANNER_LIB_BRANCH"
  cd ..
fi

# Build the gRPC server binaries.
cd go-sql-spanner/spannerlib/grpc-server
./build-executables.sh
cd ../../..

# Copy the built binaries into this project.
copy_binary() {
  local platform=$1 # e.g., osx-arm64
  local ext=${2:-}
  local src_dir="go-sql-spanner/spannerlib/grpc-server/binaries/${platform}"
  local dest_dir="runtimes/${platform}/native"
  local filename="spannerlib_grpc_server${ext}"

  mkdir -p "${dest_dir}"
  cp "${src_dir}/${filename}" "${dest_dir}/${filename}"
}

copy_binary "osx-arm64"
copy_binary "linux-x64"
copy_binary "linux-arm64"
copy_binary "win-x64" ".exe"

rm -rf go-sql-spanner
