using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

public class SpannerSqlNullabilityProcessor : SqlNullabilityProcessor
{
    public SpannerSqlNullabilityProcessor([NotNull] RelationalParameterBasedSqlProcessorDependencies dependencies, bool useRelationalNulls) :
        base(dependencies, useRelationalNulls)
    {
    }

    protected override SqlExpression VisitCustomSqlExpression(
        SqlExpression sqlExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        _ = sqlExpression switch
        {
            SpannerValueExpression valueExpression => Visit(valueExpression.Value, allowOptimizedExpansion, out nullable),
            _ => base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable),
        };
        return sqlExpression;
    }
    
}