#!/bin/bash

# Environment variables:
# - COMMITTISH_OVERRIDE: The commit to actually build the release from, if not the one that has been checked out
# - SKIP_NUGET_PUSH: If non-empty, the push to nuget.org is skipped
# - SKIP_PAGES_UPLOAD: If non-empty, the push to gh-pages is skipped
# - SKIP_GOOGLEAPISDEV_UPLOAD: If non-empty, the push to googleapis.dev is skipped

set -e

SCRIPT=$(readlink -f "$0")
SCRIPT_DIR=$(dirname "$SCRIPT")

cd $SCRIPT_DIR
cd ..

source $SCRIPT_DIR/populatesecrets.sh

# Only populate secrets if we have to.
# Else, we assume secrets have already been populated by the caller.
populatesecrets=true
if [[ "$#" -eq 1 ]] && [[ "$1" == "--skippopulatesecrets" ]]
then
    populatesecrets=false
    echo "Skipping populate secrets."
elif [[ "$#" -gt 0 ]]
then
    echo "Usage: $0 [--skippopulatesecrets]"
    exit 1
fi
if [[ "$populatesecrets" == "true" ]]
then
    populate_all_secrets
fi

export GOOGLE_APPLICATION_CREDENTIALS="$SECRETS_LOCATION/cloud-sharp-jenkins-compute-service-account"
export REQUESTER_PAYS_CREDENTIALS="$SECRETS_LOCATION/gcloud-devel-service-account"

PYTHON3=$(source toolversions.sh && echo $PYTHON3)
DOCS_CREDENTIALS="$SECRETS_LOCATION/docuploader_service_account"
GOOGLE_CLOUD_NUGET_API_KEY="$(cat "$SECRETS_LOCATION"/google-cloud-nuget-api-key)"
GOOGLE_APIS_PACKAGES_NUGET_API_KEY="$(cat "$SECRETS_LOCATION"/google-apis-nuget-api-key)"

COMMITTISH=$COMMITTISH_OVERRIDE
if [[ $COMMITTISH_OVERRIDE = "" ]]
then
  COMMITTISH=$(git rev-parse HEAD)
else
  COMMITTISH=$COMMITTISH_OVERRIDE
fi

echo "Building with commit $COMMITTISH"

# Build the release and run the tests.
./buildrelease.sh --ssh $COMMITTISH

if [[ $SKIP_NUGET_PUSH = "" ]]
then
  echo "Pushing NuGet packages"
  # Push the changes to nuget.
  cd ./releasebuild/nuget
  for pkg in *.nupkg
  do
    package_owner="google-cloud"
    pkg_nuget_api_key=$GOOGLE_CLOUD_NUGET_API_KEY

    dotnet nuget push -s https://api.nuget.org/v3/index.json -k $pkg_nuget_api_key $pkg
  done
  cd ../..
else
  echo "Skipping NuGet push"
fi

# TODO: Setup documentation push.
