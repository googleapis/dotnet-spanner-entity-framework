#!/bin/bash

set -e

cd $(dirname $0)

source toolversions.sh

# Make it easier to handle globbing that doesn't
# match anything, e.g. when looking for tests.
shopt -s nullglob

# Command line arguments are the APIs to build. Each argument
# should be the name of a directory, either relative to the location
# of this script, or under apis.
# Additional arguments:
# --notests: Just build, don't run the tests
# --nobuild: Just list which APIs would be built; don't run the build
# --coverage: Run tests with coverage enabled
apis=()
runtests=true
runcoverage=false
apiregex=
nobuild=false
diff=false
while (( "$#" )); do
  if [[ "$1" == "--notests" ]]
  then
    runtests=false
  elif [[ "$1" == "--nobuild" ]]
  then
    nobuild=true
  elif [[ "$1" == "--coverage" ]]
  then
    runcoverage=true
    install_dotcover
    mkdir -p coverage
  else
    apis+=($1)
  fi
  shift
done

# Build and test the tools, but only on Windows
[[ "$OS" == "Windows_NT" ]] && tools="tools" || tools=""

if [[ "$nobuild" == "true" ]]
then
  echo "APIs that would be built:"
  for api in ${apis[*]}
  do
    echo "$api"
  done
  exit 0
fi

# TODO: Do we need these?
# First build the analyzers, for use in everything else.
#log_build_action "(Start) build.sh"
#log_build_action "Building analyzers"

#dotnet publish -nologo -clp:NoSummary -v quiet -c Release -f netstandard2.0 tools/Google.Cloud.Tools.Analyzers

# Then build the requested APIs, working out the test projects as we go.
> AllTests.txt
for api in ${apis[*]}
do
  apidir=$api

  log_build_action "Building $apidir"
  dotnet build -nologo -clp:NoSummary -v quiet -c Release $apidir

  # On Linux, we don't have desktop .NET, so any projects which only
  # support desktop .NET are going to be broken. Just don't add them.
  for testproject in $apidir.Tests/*.csproj
  do
    if [[ "$OS" == "Windows_NT" ]] || ! grep -q -E '>net[0-9]+<' $testproject
    then
      echo "$testproject" >> AllTests.txt
    fi
  done
  
  # If we're not going to test the desktop .NET builds, let's remove them
  # entirely. This saves a huge amount of disk space, as the desktop framework
  # builds include copies of gRPC.
  if [[ ! "$OS" == "Windows_NT" ]]
  then
    rm -rf $apidir/bin/Release/net[0-9]*
  fi
done

if [[ "$runtests" = true ]]
then
  log_build_action "(Start) Unit tests"
  # Could use xargs, but this is more flexible
  while read testproject
  do
    testdir=$(dirname $testproject)
    log_build_action "Testing $testdir"
    if [[ "$runcoverage" = true && -f "$testdir/coverage.xml" ]]
    then
      echo "(Running with coverage)"
      (cd "$testdir"; $DOTCOVER cover "coverage.xml" --ReturnTargetExitCode)
    else
      dotnet test -nologo -c Release --no-build $testproject
    fi
  done < AllTests.txt
  log_build_action "(End) Unit tests"
fi

log_build_action "(End) build.sh"
