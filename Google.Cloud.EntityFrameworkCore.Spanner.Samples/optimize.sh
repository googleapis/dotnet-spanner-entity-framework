#!/bin/bash

RESULT="$(dotnet ef dbcontext optimize --output-dir CompiledModels --namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.CompiledModels --context SpannerSampleDbContext)"
if [[ $RESULT == *"Successfully generated a compiled model"* ]]; then
  echo "Successfully generated a compiled model"
  exit 0
else
  echo $RESULT
  exit 1
fi
