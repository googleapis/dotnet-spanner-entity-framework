// Copyright 2024 Google LLC
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

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

/// <summary>
/// Generates an `(NOT) IN UNNEST (@param)`-style IN expression.
/// </summary>
internal class SpannerInExpression : InExpression
{
    private static readonly ConstructorInfo s_quotingConstructor =
        typeof(SpannerInExpression).GetConstructor(
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            new[] { typeof(SqlExpression), typeof(SqlParameterExpression), typeof(RelationalTypeMapping) },
            null)!;

    internal SpannerInExpression(
        SqlExpression item,
        SqlParameterExpression valuesParameter,
        RelationalTypeMapping typeMapping)
        : base(item, valuesParameter, typeMapping)
    {
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var item = (SqlExpression)visitor.Visit(Item);
        var valuesParameter = (SqlParameterExpression)visitor.Visit(ValuesParameter);

        return Update(item, valuesParameter);
    }

    public override InExpression Update(SqlExpression item, SqlParameterExpression valuesParameter)
        => item != Item || valuesParameter != ValuesParameter
            ? new SpannerInExpression(item, valuesParameter, TypeMapping)
            : this;

    public override InExpression Update(
        SqlExpression item,
        SelectExpression subquery,
        IReadOnlyList<SqlExpression> values,
        SqlParameterExpression valuesParameter)
    {
        if (subquery == null && values == null && valuesParameter != null)
        {
            return Update(item, valuesParameter);
        }

        return base.Update(item, subquery, values, valuesParameter);
    }

    public override InExpression ApplyTypeMapping(RelationalTypeMapping typeMapping)
        => new SpannerInExpression(Item, ValuesParameter, typeMapping);

#pragma warning disable EF9100
    public override Expression Quote()
        => Expression.New(
            s_quotingConstructor,
            Item.Quote(),
            ValuesParameter.Quote(),
            RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));
#pragma warning restore EF9100

    public override bool Equals(object obj)
        => obj != null
            && (ReferenceEquals(this, obj)
                || obj is SpannerInExpression inExpression
                    && base.Equals(inExpression));

    public override int GetHashCode()
        => base.GetHashCode();

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Item);
        expressionPrinter.Append(" IN UNNEST (");
        expressionPrinter.Visit(ValuesParameter);
        expressionPrinter.Append(")");
    }
}
