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
    public SpannerSqlNullabilityProcessor([NotNull] RelationalParameterBasedSqlProcessorDependencies dependencies, RelationalParameterBasedSqlProcessorParameters parameters) :
        base(dependencies, parameters)
    {
    }

    /// <inheritdoc />
    protected override SqlExpression VisitCustomSqlExpression(
        SqlExpression sqlExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
        => sqlExpression switch
        {
            SpannerValueExpression valueExpression => VisitSpannerValue(valueExpression, allowOptimizedExpansion, out nullable),
            SpannerContainsExpression containsExpression => VisitSpannerContains(containsExpression, allowOptimizedExpansion, out nullable),
            _ => base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable),
        };

    protected override SqlExpression VisitIn(
        InExpression inExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        if (inExpression is SpannerInExpression spannerInExpression)
        {
            var item = Visit(spannerInExpression.Item, out var itemNullable);
            var valuesParameter = (SqlParameterExpression)Visit(spannerInExpression.ValuesParameter, out _);
            var updated = (SpannerInExpression)spannerInExpression.Update(item, valuesParameter);

            return ProcessInNullability(updated, item, itemNullable, allowOptimizedExpansion, out nullable);
        }

        return base.VisitIn(inExpression, allowOptimizedExpansion, out nullable);
    }

    private SqlExpression VisitSpannerValue(
        SpannerValueExpression valueExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        var value = Visit(valueExpression.Value, allowOptimizedExpansion, out nullable);
        return valueExpression.Update(value);
    }

    /// <summary>
    /// Processes nullability for <see cref="SpannerContainsExpression"/> (<c>item IN UNNEST(values)</c>).
    /// Cloud Spanner follows ANSI 3-valued logic: when <c>item</c> is NULL, <c>NULL IN UNNEST(...)</c> evaluates to NULL (UNKNOWN).
    /// This method compensates for the difference between C# 2-valued boolean semantics and SQL 3-valued logic.
    /// <para>
    /// Note on collections with NULL elements: Cloud Spanner retains parameterized array expressions (<c>IN UNNEST(@param)</c>)
    /// to avoid parameter expansion and query plan cache bloat. In GoogleSQL, <c>NULL IN UNNEST([..., NULL])</c> evaluates
    /// to UNKNOWN (treated as false in a WHERE clause).
    /// </para>
    /// </summary>
    protected virtual SqlExpression VisitSpannerContains(
        SpannerContainsExpression containsExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        // Note: containsExpression.IsNegated is always false here because in EF Core,
        // boolean negation (!list.Contains(...)) is represented by an outer SqlUnaryExpression.Not.
        var item = Visit(containsExpression.Item, out var itemNullable);
        var values = Visit(containsExpression.Values, out _);
        var updated = containsExpression.Update(item, values);

        return ProcessInNullability(updated, item, itemNullable, allowOptimizedExpansion, out nullable);
    }

    private SqlExpression ProcessInNullability(
        SqlExpression expression,
        SqlExpression item,
        bool itemNullable,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        // When relational nulls are enabled (UseRelationalNulls), C# null-compensation is disabled,
        // so we return the raw SQL expression directly.
        if (UseRelationalNulls)
        {
            nullable = itemNullable;
            return expression;
        }

        // If the item cannot be null at runtime, no null-safety compensation is required.
        if (!itemNullable)
        {
            nullable = false;
            return expression;
        }

        // For contains/in inside a WHERE predicate (allowOptimizedExpansion is true):
        // In SQL WHERE clauses, NULL evaluates to false (not included), which matches C# false.
        // Therefore, we can omit the redundant `AND item IS NOT NULL` check, emitting optimal SQL.
        if (allowOptimizedExpansion)
        {
            nullable = true;
            return expression;
        }

        // For contains/in outside a WHERE predicate (allowOptimizedExpansion is false, e.g. in a SELECT projection,
        // or when negated under NOT in a WHERE clause where VisitSqlUnary passes allowOptimizedExpansion: false):
        // 1. In projections: If item is NULL, SQL produces NULL. Reading DBNull into a non-nullable C# bool causes an InvalidCastException.
        //    Rewriting to `(expression) AND (item IS NOT NULL)` evaluates to false.
        // 2. Under NOT: VisitSqlUnary negates this expression using De Morgan's laws, producing:
        //    `(NOT expression) OR (item IS NULL)`, which correctly includes NULL rows in C# !values.Contains(item).
        nullable = false;
        return Dependencies.SqlExpressionFactory.AndAlso(
            expression,
            Dependencies.SqlExpressionFactory.IsNotNull(item));
    }
}
