using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Collections.Generic;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

public class SpannerParameterBasedSqlProcessor : RelationalParameterBasedSqlProcessor
{
    public SpannerParameterBasedSqlProcessor([NotNull] RelationalParameterBasedSqlProcessorDependencies dependencies, bool useRelationalNulls) :
        base(dependencies, useRelationalNulls)
    {
    }
    
    protected override SelectExpression ProcessSqlNullability(
        SelectExpression selectExpression,
        IReadOnlyDictionary<string, object?> parametersValues,
        out bool canCache)
        => new SpannerSqlNullabilityProcessor(Dependencies, UseRelationalNulls).Process(
            selectExpression, parametersValues, out canCache);
}