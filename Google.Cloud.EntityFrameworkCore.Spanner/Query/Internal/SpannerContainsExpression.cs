﻿// Copyright 2022, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#nullable enable

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

/// <summary>
/// Only for internal use.
/// </summary>
public class SpannerContainsExpression : SqlExpression
{
    private static ConstructorInfo? _quotingConstructorWithValues;
    
    public SqlExpression Values { get; }
    public SqlExpression Item { get; }
    public virtual bool IsNegated { get; }
    
    internal SpannerContainsExpression(
        SqlExpression item,
        SqlExpression values,
        bool negated, 
        RelationalTypeMapping? typeMapping)
        : base(typeof(bool), typeMapping)
    {
        Item = item;
        Values = values;
        IsNegated = negated;
    }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var item = (SqlExpression)visitor.Visit(Item);
        var values = (SqlExpression)visitor.Visit(Values);

        return Update(item, values);
    }


    /// <inheritdoc />
    public override Expression Quote()
#pragma warning disable EF9100 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
        => this switch
        {
            { Values: not null } => New(
                _quotingConstructorWithValues ??= typeof(SpannerContainsExpression).GetConstructor(
                    [typeof(SqlExpression), typeof(SqlExpression), typeof(bool), typeof(RelationalTypeMapping)])!,
                Item.Quote(),
                Values.Quote(),
                Constant(IsNegated),
                RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping)),

            _ => throw new UnreachableException()
        };
#pragma warning restore EF9100 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Visit(Item);
        expressionPrinter.Append(IsNegated ? " NOT IN " : " IN ");
        expressionPrinter.Append("(");
        
        if (Values is SqlConstantExpression constantValuesExpression
                 && constantValuesExpression.Value is IEnumerable constantValues)
        {
            var first = true;
            foreach (var item in constantValues)
            {
                if (!first)
                {
                    expressionPrinter.Append(", ");
                }

                first = false;
                expressionPrinter.Append(
                    constantValuesExpression.TypeMapping?.GenerateSqlLiteral(item)
                    ?? item?.ToString()
                    ?? "NULL");
            }
        }
        else
        {
            expressionPrinter.Visit(Values);
        }

        expressionPrinter.Append(")");
    }
    
    /// <inheritdoc />
    public override bool Equals(object? obj)
        => obj is not null
           && (ReferenceEquals(this, obj)
               || obj is SpannerContainsExpression containsExpression
               && Equals(containsExpression));

    private bool Equals(SpannerContainsExpression containsExpression)
        => base.Equals(containsExpression)
           && Item.Equals(containsExpression.Item)
           && IsNegated.Equals(containsExpression.IsNegated)
           && Values.Equals(containsExpression.Values);

    /// <inheritdoc />
    public override int GetHashCode()
        => HashCode.Combine(base.GetHashCode(), Item, IsNegated, Values);
    
    public virtual SpannerContainsExpression Update(
        SqlExpression item,
        SqlExpression values
        )
    {
        return item != Item 
               || values != Values
            ? new SpannerContainsExpression(item, values, IsNegated, TypeMapping)
            : this;
    }
}
