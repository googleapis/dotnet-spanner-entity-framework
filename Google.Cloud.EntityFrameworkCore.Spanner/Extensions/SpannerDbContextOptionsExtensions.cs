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
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using System;

namespace Microsoft.EntityFrameworkCore
{
    /// <summary>
    /// Spanner specific extension methods for <see cref="DbContextOptionsBuilder" />.
    /// </summary>
    public static class SpannerDbContextOptionsExtensions
    {
        /// <param name="optionsBuilder"></param>
        /// <param name="connectionString"></param>
        /// <param name="spannerOptionsAction"></param>
        /// <returns></returns>
        public static DbContextOptionsBuilder UseSpanner(
            this DbContextOptionsBuilder optionsBuilder,
            string connectionString,
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null)
        {
            GaxPreconditions.CheckNotNull(optionsBuilder, nameof(optionsBuilder));
            GaxPreconditions.CheckNotNullOrEmpty(connectionString, nameof(connectionString));

            var extension = (SpannerOptionsExtension)GetOrCreateExtension(optionsBuilder)
                .WithConnectionString(connectionString);
            ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

            ConfigureWarnings(optionsBuilder);
            spannerOptionsAction?.Invoke(new SpannerDbContextOptionsBuilder(optionsBuilder));

            return optionsBuilder;
        }

        public static DbContextOptionsBuilder UseSpanner(
            this DbContextOptionsBuilder optionsBuilder,
            SpannerConnection connection,
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null) =>
            UseSpanner(optionsBuilder, new SpannerRetriableConnection(connection), spannerOptionsAction);

        /// <summary>
        /// </summary>
        /// <param name="optionsBuilder"></param>
        /// <param name="connection"></param>
        /// <param name="spannerOptionsAction"></param>
        /// <returns></returns>
        internal static DbContextOptionsBuilder UseSpanner(
            this DbContextOptionsBuilder optionsBuilder,
            SpannerRetriableConnection connection,
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null)
        {
            GaxPreconditions.CheckNotNull(optionsBuilder, nameof(optionsBuilder));
            GaxPreconditions.CheckNotNull(connection, nameof(connection));

            var extension = (SpannerOptionsExtension)GetOrCreateExtension(optionsBuilder).WithConnection(connection);
            ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

            ConfigureWarnings(optionsBuilder);

            spannerOptionsAction?.Invoke(new SpannerDbContextOptionsBuilder(optionsBuilder));

            return optionsBuilder;
        }

        /// <summary>
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="optionsBuilder"></param>
        /// <param name="connectionString"></param>
        /// <param name="spannerOptionsAction"></param>
        /// <returns></returns>
        public static DbContextOptionsBuilder<TContext> UseSpanner<TContext>(
            this DbContextOptionsBuilder<TContext> optionsBuilder,
            string connectionString,
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null)
            where TContext : DbContext
            => (DbContextOptionsBuilder<TContext>)UseSpanner(
                (DbContextOptionsBuilder)optionsBuilder, connectionString, spannerOptionsAction);

        public static DbContextOptionsBuilder<TContext> UseSpanner<TContext>(
            this DbContextOptionsBuilder<TContext> optionsBuilder,
            SpannerConnection connection,
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null)
            where TContext : DbContext
            => (DbContextOptionsBuilder<TContext>)UseSpanner(
                (DbContextOptionsBuilder)optionsBuilder, new SpannerRetriableConnection(connection), spannerOptionsAction);

        internal static DbContextOptionsBuilder<TContext> UseSpanner<TContext>(
            this DbContextOptionsBuilder<TContext> optionsBuilder,
            SpannerRetriableConnection connection,
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null)
            where TContext : DbContext
            => (DbContextOptionsBuilder<TContext>)UseSpanner(
                (DbContextOptionsBuilder)optionsBuilder, connection, spannerOptionsAction);

        public static DbContextOptionsBuilder UseMutations(
            this DbContextOptionsBuilder optionsBuilder,
            MutationUsage mutationUsage)
        {
            GaxPreconditions.CheckNotNull(optionsBuilder, nameof(optionsBuilder));
            var extension = GetOrCreateExtension(optionsBuilder).WithMutationUsage(mutationUsage);
            ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

            return optionsBuilder;
        }

        private static SpannerOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.Options.FindExtension<SpannerOptionsExtension>()
               ?? new SpannerOptionsExtension();

        private static void ConfigureWarnings(DbContextOptionsBuilder optionsBuilder)
        {
            var coreOptionsExtension
                = optionsBuilder.Options.FindExtension<CoreOptionsExtension>()
                  ?? new CoreOptionsExtension();

            coreOptionsExtension = coreOptionsExtension.WithWarningsConfiguration(
                coreOptionsExtension.WarningsConfiguration.TryWithExplicit(
                    RelationalEventId.AmbientTransactionWarning, WarningBehavior.Throw));

            ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(coreOptionsExtension);
        }
    }
}
