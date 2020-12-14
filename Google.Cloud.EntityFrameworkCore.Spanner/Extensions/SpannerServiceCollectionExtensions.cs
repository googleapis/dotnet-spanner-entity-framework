using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Update.Internal;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>
    /// Spanner specific extension methods for <see cref="IServiceCollection" />.
    /// </summary>
    public static class SpannerServiceCollectionExtensions
    {
        /// <summary>
        /// Adds base EFCore services along with Cloud Spanner specific services.
        /// </summary>
        public static IServiceCollection AddEntityFrameworkSpanner(this IServiceCollection serviceCollection)
        {
            GaxPreconditions.CheckNotNull(serviceCollection, nameof(serviceCollection));

            var builder = new EntityFrameworkRelationalServicesBuilder(serviceCollection)
                .TryAdd<LoggingDefinitions, SpannerLoggingDefinitions>()
                .TryAdd<IDatabaseProvider, DatabaseProvider<SpannerOptionsExtension>>()
                .TryAdd<IRelationalTypeMappingSource, SpannerTypeMappingSource>()
                .TryAdd<ISqlGenerationHelper, SpannerSqlGenerationHelper>()
                .TryAdd<IModificationCommandBatchFactory, SpannerModificationCommandBatchFactory>()
                .TryAdd<IModelValidator, RelationalModelValidator>()
                .TryAdd<IQuerySqlGeneratorFactory, SpannerQuerySqlGeneratorFactory>()
                .TryAdd<IRelationalConnection>(p => p.GetService<ISpannerRelationalConnection>())
                .TryAdd<IExecutionStrategyFactory, RelationalExecutionStrategyFactory>()
                  .TryAddProviderSpecificServices(b => b
                    .TryAddScoped<ISpannerRelationalConnection, SpannerRelationalConnection>());
            builder.TryAddCoreServices();
            return serviceCollection;
        }
    }
}
