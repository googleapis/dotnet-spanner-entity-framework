// Copyright 2020, Google Inc. All rights reserved.
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

using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Diagnostics.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions
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
                .TryAdd<IMigrationsAnnotationProvider, SpannerMigrationsAnnotationProvider>()
                .TryAdd<IUpdateSqlGenerator, SpannerUpdateSqlGenerator>()
                .TryAdd<IBatchExecutor, SpannerBatchExecutor>()
                .TryAdd<IModificationCommandBatchFactory, SpannerModificationCommandBatchFactory>()
                .TryAdd<IModelValidator, RelationalModelValidator>()
                .TryAdd<IQuerySqlGeneratorFactory, SpannerQuerySqlGeneratorFactory>()
                .TryAdd<IMethodCallTranslatorProvider, SpannerMethodCallTranslatorProvider>()
                .TryAdd<IMemberTranslatorProvider, SpannerMemberTranslatorProvider>()
                .TryAdd<IRelationalConnection>(p => p.GetService<ISpannerRelationalConnection>())
                .TryAdd<IMigrationsSqlGenerator, SpannerMigrationsSqlGenerator>()
                .TryAdd<IMigrationCommandExecutor, SpannerMigrationCommandExecutor>()
                .TryAdd<IRelationalDatabaseCreator, SpannerDatabaseCreator>()
                .TryAdd<IHistoryRepository, SpannerHistoryRepository>()
                .TryAdd<IExecutionStrategyFactory, RelationalExecutionStrategyFactory>()
                  .TryAddProviderSpecificServices(b => b
                  .TryAddScoped<ISpannerRelationalConnection, SpannerRelationalConnection>()
                );
            builder.TryAddCoreServices();
            return serviceCollection;
        }
    }
}
