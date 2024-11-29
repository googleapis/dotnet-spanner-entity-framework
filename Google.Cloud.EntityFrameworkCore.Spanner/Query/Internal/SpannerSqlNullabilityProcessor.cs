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

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

/// <inheritdoc />
public class SpannerSqlNullabilityProcessor : SqlNullabilityProcessor
{
    /// <summary>
    /// Only for internal use.
    /// </summary>
    public SpannerSqlNullabilityProcessor([NotNull] RelationalParameterBasedSqlProcessorDependencies dependencies, bool useRelationalNulls) :
        base(dependencies, useRelationalNulls)
    {
    }

    /// <inheritdoc />
    protected override SqlExpression VisitCustomSqlExpression(
        SqlExpression sqlExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        _ = sqlExpression switch
        {
            SpannerValueExpression valueExpression => Visit(valueExpression.Value, allowOptimizedExpansion, out nullable),
            SpannerContainsExpression containsExpression => VisitSpannerContains(containsExpression, out nullable),
            _ => base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable),
        };
        return sqlExpression;
    }

    protected override SqlExpression VisitIn(
        InExpression inExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        if (inExpression.GetType() == typeof(SpannerInExpression))
        {
            nullable = false;
            return inExpression;
        }
        return base.VisitIn(inExpression, allowOptimizedExpansion, out nullable);
    }

    protected virtual SqlExpression VisitSpannerContains(SpannerContainsExpression containsExpression, out bool nullable)
    {
        var item = Visit(containsExpression.Item, out var itemNullable);
        var values = Visit(containsExpression.Values, out var _);
        nullable = false;
        return containsExpression.Update(item, values);
    }
}