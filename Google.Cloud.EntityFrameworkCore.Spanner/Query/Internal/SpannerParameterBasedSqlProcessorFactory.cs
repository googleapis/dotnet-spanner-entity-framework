using Microsoft.EntityFrameworkCore.Query;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

public class SpannerParameterBasedSqlProcessorFactory : IRelationalParameterBasedSqlProcessorFactory
{
    public SpannerParameterBasedSqlProcessorFactory(RelationalParameterBasedSqlProcessorDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    protected virtual RelationalParameterBasedSqlProcessorDependencies Dependencies { get; }

    public virtual RelationalParameterBasedSqlProcessor Create(bool useRelationalNulls)
        => new SpannerParameterBasedSqlProcessor(Dependencies, useRelationalNulls);
}
