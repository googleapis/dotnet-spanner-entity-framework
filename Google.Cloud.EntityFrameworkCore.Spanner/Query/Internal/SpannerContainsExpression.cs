#nullable enable

using System;
using System.Collections;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

public class SpannerContainsExpression : SqlExpression
{
    public SqlExpression Values { get; }
    public SqlExpression Item { get; }
    public virtual bool IsNegated { get; }
    
    public SpannerContainsExpression(
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
                expressionPrinter.Append(constantValuesExpression.TypeMapping?.GenerateSqlLiteral(item) ?? item?.ToString() ?? "NULL");
            }
        }
        else
        {
            expressionPrinter.Visit(Values);
        }

        expressionPrinter.Append(")");
    }
    
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