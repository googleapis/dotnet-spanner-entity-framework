using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

public abstract class SpannerValueExpression : SqlExpression
{
    protected SpannerValueExpression([NotNull] System.Type type, [CanBeNull] RelationalTypeMapping? typeMapping) : base(type, typeMapping)
    {
    }

    internal abstract SqlExpression Value { get; }
}
