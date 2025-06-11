// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

/// <inheritdoc />
public class SpannerParameterBasedSqlProcessor : RelationalParameterBasedSqlProcessor
{
    /// <summary>
    /// Only for internal use.
    /// </summary>
    public SpannerParameterBasedSqlProcessor([NotNull] RelationalParameterBasedSqlProcessorDependencies dependencies, RelationalParameterBasedSqlProcessorParameters parameters) :
        base(dependencies, parameters)
    {
    }
    
    /// <inheritdoc />
    protected override Expression ProcessSqlNullability(
        Expression selectExpression,
        [ItemCanBeNull] IReadOnlyDictionary<string, object> parametersValues,
        out bool canCache)
        => new SpannerSqlNullabilityProcessor(Dependencies, Parameters).Process(
            selectExpression, parametersValues, out canCache);
}