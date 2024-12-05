#!/bin/bash

# Script to perform a release build for all APIs tagged at
# a specific commit. A single argument is required: the commit to use.

# The repo is cloned into a fresh "releasebuild" directory.

set -e

# Additional arguments:
# --rebuild_docs: Just build the projects and docs, don't create nuget packages.
#                 Also, use the latest to pick up new docs changes rather than
#                 the commit of the tag.
# --skip_tests: Skip the integration tests
# --ssh: Use SSH to clone GitHub repos
rebuild_docs=false
run_tests=true
clone_path_prefix="https://github.com/"
commit=
while (( "$#" )); do
  if [[ "$1" == "--rebuild_docs" ]]
  then
    rebuild_docs=true
  elif [[ "$1" == "--skip_tests" ]]
  then
    run_tests=false
  elif [[ "$1" == "--ssh" ]]
  then
    clone_path_prefix="git@github.com:"
  else
    commit=$1
  fi
  shift
done

if [ -z "$commit" ]
then
  echo "Please specify a commit hash"
  exit 1
fi

# Do everything from the repository root for sanity.
cd $(dirname $0)

rm -rf releasebuild
git clone ${clone_path_prefix}googleapis/dotnet-spanner-entity-framework.git releasebuild -c core.autocrlf=input --recursive
cd releasebuild

# See b/381899554
git config --global --add safe.directory C:/tmpfs/src/github/dotnet-spanner-entity-framework

# Make sure the package is deterministic. We don't do this for local builds,
# but it makes debugging work more reliably for PDBs in packages.
export DeterministicSourcePaths=true

if [[ "$rebuild_docs" = true ]]
then
  git checkout master
else
  git checkout $commit
fi

# Turn the multi-line output of git tag --points-at into space-separated list of projects
projects="Google.Cloud.EntityFrameworkCore.Spanner"

./build.sh $projects

# TODO: Figure out how to get the integration tests to run.

if [[ "$rebuild_docs" = false ]]
then
  for project in $projects
  do
    # Don't pack the whole solution - just the project. (Avoids packing dependent
    # projects such as Google.LongRunning.)
    dotnet pack --no-build --no-restore -o $PWD/nuget -c Release $project
  done
fi

# TODO: Use builddocs.sh to build docs.

echo "Release build and docs complete for the following projects:"
for project in $projects
do
  echo "- ${project}"
done
if [[ "$rebuild_docs" = false ]]
then
  echo "- Push packages to nuget:"
  echo "  - cd releasebuild/nuget"
  echo "  - Remove any packages you don't want to push"
  echo "  - for pkg in *.nupkg; do dotnet nuget push -s https://api.nuget.org/v3/index.json -k API_KEY_HERE \$pkg; done"
fi
