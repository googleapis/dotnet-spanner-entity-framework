using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.Extensions.Logging;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    internal class SpannerMethodCallTranslatorProvider : RelationalMethodCallTranslatorProvider
    {
        public SpannerMethodCallTranslatorProvider(
            [NotNull] RelationalMethodCallTranslatorProviderDependencies dependencies)
            : base(dependencies)
        {
            var sqlExpressionFactory = (SpannerSqlExpressionFactory) dependencies.SqlExpressionFactory;

            AddTranslators(
                new IMethodCallTranslator[]
                {
                    new SpannerNullableMethodTranslator(sqlExpressionFactory),
                    new SpannerObjectToStringTranslator(sqlExpressionFactory),
                    new SpannerStringMethodTranslator(sqlExpressionFactory),
                    new SpannerRegexMethodTranslator(sqlExpressionFactory),
                    new SpannerDateTimeMethodTranslator(sqlExpressionFactory),
                    new SpannerMathTranslator(sqlExpressionFactory),
                    new SpannerListMethodCallTranslator(sqlExpressionFactory),
                });
        }
    }
}
