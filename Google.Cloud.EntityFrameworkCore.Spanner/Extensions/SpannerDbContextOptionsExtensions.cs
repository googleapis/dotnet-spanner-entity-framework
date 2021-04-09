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
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
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
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null,
            ChannelCredentials channelCredentials = null)
        {
            GaxPreconditions.CheckNotNull(optionsBuilder, nameof(optionsBuilder));
            GaxPreconditions.CheckNotNullOrEmpty(connectionString, nameof(connectionString));

            ModelValidationConnectionStringProvider.Instance.SetConnectionString(connectionString, channelCredentials);
            var extension = GetOrCreateExtension(optionsBuilder);
            if (channelCredentials == null)
            {
                extension = (SpannerOptionsExtension)extension.WithConnectionString(connectionString);
            }
            else
            {
                extension = (SpannerOptionsExtension)extension.WithConnection(new SpannerRetriableConnection(new SpannerConnection(connectionString, channelCredentials)));
            }
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
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null,
            ChannelCredentials channelCredentials = null)
        {
            GaxPreconditions.CheckNotNull(optionsBuilder, nameof(optionsBuilder));
            GaxPreconditions.CheckNotNull(connection, nameof(connection));

            ModelValidationConnectionStringProvider.Instance.SetConnectionString(connection.ConnectionString, channelCredentials);
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
                optionsBuilder, new SpannerRetriableConnection(connection), spannerOptionsAction);

        /// <summary>
        /// This option is intended for advanced users.
        /// 
        /// Configure when the DbContext should use mutations instead of DML. The default configuration
        /// will use mutations for implicit transactions and DML for manual transactions. The default
        /// configuration is the best options for most use cases.
        /// 
        /// This option can be set to MutationUsage.Always if your application experiences slow performance
        /// for manual transactions that execute a large number of inserts/updates/deletes. Note that
        /// setting this option to MutationUsage.Always will disable read-your-writes for manual transactions
        /// in the DbContext.
        /// </summary>
        /// <param name="optionsBuilder">the optionsBuilder to configure</param>
        /// <param name="mutationUsage">the configuration option to use for the DbContext</param>
        /// <returns>the optionsBuilder</returns>
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
