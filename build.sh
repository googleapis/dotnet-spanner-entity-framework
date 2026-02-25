#!/bin/bash

set -e

export ContinuousIntegrationBuild=true
export Configuration=Release

echo Building Google.Cloud.EntityFrameworkCore.Spanner...
dotnet build -nologo -clp:NoSummary -v quiet Google.Cloud.EntityFrameworkCore.Spanner

echo Testing...
dotnet test -nologo -v quiet Google.Cloud.EntityFrameworkCore.Spanner.Tests

echo Packing...
rm -rf nupkg
dotnet pack -nologo -v quiet Google.Cloud.EntityFrameworkCore.Spanner -o $PWD/nupkg

# Download and install Go
GO_VERSION="1.26.0"
MSI_FILE="go${GO_VERSION}.windows-amd64.msi"
DOWNLOAD_URL="https://go.dev/dl/${MSI_FILE}"

echo "Downloading Go ${GO_VERSION}..."
curl -fL -o "$MSI_FILE" "$DOWNLOAD_URL"

echo "Installing Go silently"
# /i = install, /quiet = no UI, /norestart = don't reboot the machine
msiexec.exe //i "$MSI_FILE" //quiet //norestart

echo "Cleaning up installer file..."
rm "$MSI_FILE"
echo "Installation finished."

echo "Updating PATH for the current script session..."
# Use standard Bash export, converting the C:\ path to a Git Bash /c/ path
export PATH=$PATH:/c/Program\ Files/Go/bin
# Verify that Go is installed and works
go version

echo Building spanner-ado-net...
pushd spanner-ado-net/spanner-ado-net

# Ensure go-sql-spanner is present if not skipped by CI
if [ ! -d "go-sql-spanner" ]; then
  source spanner-lib-version.sh
  git clone https://github.com/googleapis/go-sql-spanner.git --branch "$SPANNER_LIB_BRANCH" go-sql-spanner
fi

chmod +x build-binaries.sh
./build-binaries.sh true

echo Building dotnet project...
dotnet build -c Release spanner-ado-net.csproj

echo Testing ADO.NET driver...
pushd ..
dotnet test -nologo spanner-ado-net-tests
dotnet test -nologo spanner-ado-net-specification-tests
popd

echo Packing...
dotnet pack -c Release spanner-ado-net.csproj -o ../../nupkg

echo Created packages:
ls ../../nupkg
popd
