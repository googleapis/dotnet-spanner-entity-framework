#!/bin/bash

set -e

export ContinuousIntegrationBuild=true
export Configuration=Release

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
NUPKG_DIR="$REPO_ROOT/nupkg"

# Optional argument: build a specific project (Google.Cloud.EntityFrameworkCore.Spanner | Google.Cloud.Spanner.DataProvider)
# Defaults to building all projects if not specified.
PROJECT=${1:-all}

ensure_dotnet_10() {
  # Add user-local .NET installation to PATH if it exists
  if [ -d "$HOME/.dotnet" ]; then
    export PATH="$HOME/.dotnet:$PATH"
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win"* ]]; then
      export DOTNET_ROOT="$(cygpath -w "$HOME/.dotnet")"
    else
      export DOTNET_ROOT="$HOME/.dotnet"
    fi
  fi

  # Ensure .NET 10 SDK is installed
  if ! dotnet --list-sdks 2>/dev/null | grep -q "^10\."; then
    echo "Installing .NET 10 SDK..."
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win"* ]]; then
      powershell -NoProfile -ExecutionPolicy Bypass -Command "& { \$ErrorActionPreference = 'Stop'; [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -UseBasicParsing -Uri 'https://dot.net/v1/dotnet-install.ps1' -OutFile 'dotnet-install.ps1'; & ./dotnet-install.ps1 -Channel 10.0 -Architecture x64 -InstallDir \"\$HOME\.dotnet\"; Remove-Item 'dotnet-install.ps1' -Force }"
      export DOTNET_ROOT="$(cygpath -w "$HOME/.dotnet")"
    else
      curl -sSL https://dot.net/v1/dotnet-install.sh | bash -s -- --channel 10.0
      export DOTNET_ROOT="$HOME/.dotnet"
    fi
    export PATH="$HOME/.dotnet:$PATH"
  fi
}

ensure_go() {
  # Check standard Go install locations first
  if [ -d "$HOME/go/bin" ]; then
    export PATH="$HOME/go/bin:$PATH"
  elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win"* ]] && [ -d "/c/Program Files/Go/bin" ]; then
    export PATH="$PATH:/c/Program Files/Go/bin"
  fi

  # Download and install Go if not found
  if ! command -v go > /dev/null 2>&1; then
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win"* ]]; then
      GO_VERSION="1.26.0"
      ZIP_FILE="go${GO_VERSION}.windows-amd64.zip"
      DOWNLOAD_URL="https://go.dev/dl/${ZIP_FILE}"

      echo "Downloading Go ${GO_VERSION} (portable zip)..."
      curl -fL -o "$ZIP_FILE" "$DOWNLOAD_URL"

      echo "Extracting Go to $HOME/go..."
      tar -xf "$ZIP_FILE" -C "$HOME"
      rm -f "$ZIP_FILE"

      export PATH="$HOME/go/bin:$PATH"
      echo "Go installation finished."
    else
      echo "Error: Go is not installed. Please install Go (https://go.dev/doc/install) and try again." >&2
      exit 1
    fi
  fi
  # Verify that Go is installed and works
  go version
}

build_ef_core() {
  ensure_dotnet_10

  echo Building Google.Cloud.EntityFrameworkCore.Spanner...
  dotnet build -nologo -clp:NoSummary -v quiet "$REPO_ROOT/Google.Cloud.EntityFrameworkCore.Spanner"

  echo Testing...
  dotnet test -nologo -v quiet "$REPO_ROOT/Google.Cloud.EntityFrameworkCore.Spanner.Tests"

  echo Packing...
  mkdir -p "$NUPKG_DIR"
  dotnet pack --no-build -nologo -v quiet "$REPO_ROOT/Google.Cloud.EntityFrameworkCore.Spanner" -o "$NUPKG_DIR"
}

build_ado_net() {
  ensure_go

  echo Building spanner-ado-net...
  (
    cd "$REPO_ROOT/spanner-ado-net/spanner-ado-net"

    # Ensure go-sql-spanner is present if not skipped by CI
    if [ ! -d "go-sql-spanner" ]; then
      source spanner-lib-version.sh
      git clone https://github.com/googleapis/go-sql-spanner.git --branch "$SPANNER_LIB_BRANCH" go-sql-spanner
    fi

    chmod +x build-binaries.sh
    ./build-binaries.sh true

    echo Building dotnet project...
    dotnet build -c Release spanner-ado-net.csproj
  )

  echo Testing ADO.NET driver...
  dotnet test -c Release -nologo "$REPO_ROOT/spanner-ado-net/spanner-ado-net-tests" --filter "Category!=Integration"
  dotnet test -c Release -nologo "$REPO_ROOT/spanner-ado-net/spanner-ado-net-specification-tests"

  echo Packing...
  mkdir -p "$NUPKG_DIR"
  dotnet pack --no-build -c Release "$REPO_ROOT/spanner-ado-net/spanner-ado-net/spanner-ado-net.csproj" -o "$NUPKG_DIR"
}

rm -rf "$NUPKG_DIR"
mkdir -p "$NUPKG_DIR"

case "$PROJECT" in
  Google.Cloud.EntityFrameworkCore.Spanner)
    build_ef_core
    ;;
  Google.Cloud.Spanner.DataProvider | spanner-ado-net)
    build_ado_net
    ;;
  all)
    build_ef_core
    build_ado_net
    ;;
  *)
    echo "Error: Unknown project '$PROJECT'" >&2
    echo "Usage: $0 [Google.Cloud.EntityFrameworkCore.Spanner | Google.Cloud.Spanner.DataProvider | spanner-ado-net | all]" >&2
    exit 1
    ;;
esac

echo "Created packages in $NUPKG_DIR:"
ls -la "$NUPKG_DIR"
