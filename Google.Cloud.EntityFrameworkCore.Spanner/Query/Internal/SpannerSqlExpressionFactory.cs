using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

public class SpannerSqlExpressionFactory : SqlExpressionFactory
{
    private readonly RelationalTypeMapping _boolTypeMapping;

    public SpannerSqlExpressionFactory(SqlExpressionFactoryDependencies dependencies) : base(dependencies)
    {
        _boolTypeMapping = dependencies.TypeMappingSource.FindMapping(typeof(bool), dependencies.Model)!;
    }
    
    public virtual SpannerContainsExpression SpannerContains(SqlExpression item, SqlExpression values, bool negated)
    {
        var typeMapping = item.TypeMapping ?? Dependencies.TypeMappingSource.FindMapping(item.Type, Dependencies.Model);

        item = ApplyTypeMapping(item, typeMapping);
        values = ApplyTypeMapping(values, typeMapping);

        return new SpannerContainsExpression(item, values, negated, _boolTypeMapping);
    }}