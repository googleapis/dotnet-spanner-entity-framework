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
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using System;
using System.Threading;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions
{
    /// <summary>
    /// Spanner specific extension methods for <see cref="DbContextOptionsBuilder" />.
    /// </summary>
    public static class SpannerDbContextOptionsExtensions
    {
        private static readonly Lazy<SessionPoolManager> s_sessionPoolManager = new Lazy<SessionPoolManager>(() =>
        {
            var settings = SpannerSettings.GetDefault();
            settings.VersionHeaderBuilder.AppendAssemblyVersion("efcore", typeof(SpannerDbContextOptionsExtensions));
            return SessionPoolManager.CreateWithSettings(new SessionPoolOptions(), settings);
        }, LazyThreadSafetyMode.ExecutionAndPublication);
        internal static SessionPoolManager SessionPoolManager { get; } = s_sessionPoolManager.Value;

        /// <summary>
        /// Configures a <see cref="DbContextOptionsBuilder"/> for use with Cloud Spanner.
        /// </summary>
        /// <param name="optionsBuilder">The DbContextOptionsBuilder to configure for use with Cloud Spanner</param>
        /// <param name="connectionString">The connection string to use to connect to Cloud Spanner in the format `Data Source=projects/{project}/instances/{instance}/databases/{database};[Host={hostname};][Port={portnumber}]`</param>
        /// <param name="spannerOptionsAction">Any actions that should be executed as part of configuring the options builder for Cloud Spanner</param>
        /// <param name="channelCredentials">An optional credential for operations to be performed on the Spanner database.</param>
        /// <returns>The optionsBuilder for chaining</returns>
        public static DbContextOptionsBuilder UseSpanner(
            this DbContextOptionsBuilder optionsBuilder,
            string connectionString,
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null,
            ChannelCredentials channelCredentials = null)
        {
            GaxPreconditions.CheckNotNull(optionsBuilder, nameof(optionsBuilder));
            GaxPreconditions.CheckNotNullOrEmpty(connectionString, nameof(connectionString));

            SpannerModelValidationConnectionProvider.Instance.SetConnectionString(connectionString, channelCredentials);
            var extension = GetOrCreateExtension(optionsBuilder);
            if (channelCredentials == null)
            {
                extension = (SpannerOptionsExtension)extension.WithConnectionString(connectionString);
            }
            else
            {
                // The Cloud Spanner client library does not support adding any credentials to the connection string,
                // so in that case we need to use a SpannerConnectionStringBuilder with the credentials.
                var builder = new SpannerConnectionStringBuilder(connectionString, channelCredentials)
                {
                    SessionPoolManager = SessionPoolManager,
                };
                extension = extension.WithConnectionStringBuilder(builder);
            }
            ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

            ConfigureWarnings(optionsBuilder);
            optionsBuilder.AddInterceptors(TimestampBoundHintCommandInterceptor.TimestampBoundHintInterceptor);
            optionsBuilder.AddInterceptors(TagHintCommandInterceptor.TagHintInterceptor);
            spannerOptionsAction?.Invoke(new SpannerDbContextOptionsBuilder(optionsBuilder));

            return optionsBuilder;
        }

        /// <summary>
        /// Configures a <see cref="DbContextOptionsBuilder"/> for use with Cloud Spanner.
        /// </summary>
        /// <param name="optionsBuilder">The DbContextOptionsBuilder to configure for use with Cloud Spanner</param>
        /// <param name="connection">The connection to use to connect to Cloud Spanner</param>
        /// <param name="spannerOptionsAction">Any actions that should be executed as part of configuring the options builder for Cloud Spanner</param>
        /// <returns>The optionsBuilder for chaining</returns>
        public static DbContextOptionsBuilder UseSpanner(
            this DbContextOptionsBuilder optionsBuilder,
            SpannerConnection connection,
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null) =>
            UseSpanner(optionsBuilder, new SpannerRetriableConnection(connection), spannerOptionsAction);

        /// <summary>
        /// Configures a <see cref="DbContextOptionsBuilder"/> for use with Cloud Spanner.
        /// </summary>
        /// <param name="optionsBuilder">The DbContextOptionsBuilder to configure for use with Cloud Spanner</param>
        /// <param name="connection">The connection to use to connect to Cloud Spanner</param>
        /// <param name="spannerOptionsAction">Any actions that should be executed as part of configuring the options builder for Cloud Spanner</param>
        /// <param name="channelCredentials">An optional credential for operations to be performed on the Spanner database.</param>
        /// <returns>The optionsBuilder for chaining</returns>
        internal static DbContextOptionsBuilder UseSpanner(
            this DbContextOptionsBuilder optionsBuilder,
            SpannerRetriableConnection connection,
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null,
            ChannelCredentials channelCredentials = null)
        {
            GaxPreconditions.CheckNotNull(optionsBuilder, nameof(optionsBuilder));
            GaxPreconditions.CheckNotNull(connection, nameof(connection));

            SpannerModelValidationConnectionProvider.Instance.SetConnectionString(connection.ConnectionString, channelCredentials);
            var extension = (SpannerOptionsExtension)GetOrCreateExtension(optionsBuilder).WithConnection(connection);
            ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

            ConfigureWarnings(optionsBuilder);

            spannerOptionsAction?.Invoke(new SpannerDbContextOptionsBuilder(optionsBuilder));

            return optionsBuilder;
        }

        /// <summary>
        /// Configures a <see cref="DbContextOptionsBuilder"/> for use with Cloud Spanner.
        /// </summary>
        /// <param name="optionsBuilder">The DbContextOptionsBuilder to configure for use with Cloud Spanner</param>
        /// <param name="connectionString">The connection string to use to connect to Cloud Spanner in the format `Data Source=projects/{project}/instances/{instance}/databases/{database};[Host={hostname};][Port={portnumber}]`</param>
        /// <param name="spannerOptionsAction">Any actions that should be executed as part of configuring the options builder for Cloud Spanner</param>
        /// <returns>The optionsBuilder for chaining</returns>
        public static DbContextOptionsBuilder<TContext> UseSpanner<TContext>(
            this DbContextOptionsBuilder<TContext> optionsBuilder,
            string connectionString,
            Action<SpannerDbContextOptionsBuilder> spannerOptionsAction = null)
            where TContext : DbContext
            => (DbContextOptionsBuilder<TContext>)UseSpanner(
                (DbContextOptionsBuilder)optionsBuilder, connectionString, spannerOptionsAction);

        /// <summary>
        /// Configures a <see cref="DbContextOptionsBuilder"/> for use with Cloud Spanner.
        /// </summary>
        /// <param name="optionsBuilder">The DbContextOptionsBuilder to configure for use with Cloud Spanner</param>
        /// <param name="connection">The connection to use to connect to Cloud Spanner</param>
        /// <param name="spannerOptionsAction">Any actions that should be executed as part of configuring the options builder for Cloud Spanner</param>
        /// <returns>The optionsBuilder for chaining</returns>
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
