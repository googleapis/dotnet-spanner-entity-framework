#!/bin/bash

set -e

export ContinuousIntegrationBuild=true
export Configuration=Release

echo Building...
dotnet build -nologo -clp:NoSummary -v quiet Google.Cloud.EntityFrameworkCore.Spanner

echo Testing...
dotnet test -nologo --no-build -v quiet Google.Cloud.EntityFrameworkCore.Spanner.Tests

echo Packing...
rm -rf nupkg
dotnet pack -nologo -v quiet Google.Cloud.EntityFrameworkCore.Spanner -o $PWD/nupkg

echo Created packages:
ls nupkg