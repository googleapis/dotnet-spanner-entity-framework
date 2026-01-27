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

rm SpannerLib/*.cs
rm SpannerLibGrpcImpl/*.cs
rm SpannerLibGrpcServer/*.cs
rm SpannerLibGrpcV1/*.cs

cp go-sql-spanner/spannerlib/wrappers/spannerlib-dotnet/spannerlib-dotnet/*.cs SpannerLib
cp go-sql-spanner/spannerlib/wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-impl/*.cs SpannerLibGrpcImpl
cp go-sql-spanner/spannerlib/wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-server/*.cs SpannerLibGrpcServer
cp go-sql-spanner/spannerlib/wrappers/spannerlib-dotnet/spannerlib-dotnet-grpc-v1/*.cs SpannerLibGrpcV1

rm -rf go-sql-spanner
