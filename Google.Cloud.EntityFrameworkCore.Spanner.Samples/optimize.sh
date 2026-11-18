#!/bin/bash

RESULT="$(dotnet ef dbcontext optimize --output-dir CompiledModels --namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.CompiledModels --context SpannerSampleDbContext 2>&1)"
STATUS=$?
if [[ $STATUS -eq 0 && $RESULT == *"Successfully generated a compiled model"* ]]; then
  echo "Successfully generated a compiled model"
  exit 0
else
  echo "$RESULT"
  exit 1
fi
