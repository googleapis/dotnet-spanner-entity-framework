// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Storage
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class SpannerRelationalConnectionTest
    {
        private const string TestConnectionString = "Data Source=projects/p1/instances/i1/databases/d1";

        private static SpannerRelationalConnection CreateConnection(
            string connectionString = TestConnectionString,
            ChannelCredentials channelCredentials = null,
            DdlExecutionStrategy ddlExecutionStrategy = DdlExecutionStrategy.BlockUntilCompleted,
            MutationUsage mutationUsage = MutationUsage.ImplicitTransactions,
            int? commandTimeout = null)
        {
            var serviceProvider = SpannerTestHelpers.Instance.CreateServiceProvider();
            var optionsBuilder = new DbContextOptionsBuilder()
                .UseInternalServiceProvider(serviceProvider)
                .UseSpanner(
                    connectionString,
                    options =>
                    {
                        SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false);
                        if (commandTimeout.HasValue)
                        {
                            options.CommandTimeout(commandTimeout.Value);
                        }
                    },
                    channelCredentials)
                .UseMigrationDdlExecutionStrategy(ddlExecutionStrategy)
                .UseMutations(mutationUsage);

            var context = new DbContext(optionsBuilder.Options);
            return (SpannerRelationalConnection)context.GetService<ISpannerRelationalConnection>();
        }

        [Fact]
        public void CreateMasterConnection_InheritsSettingsAndDisposesOwnedConnection()
        {
            var relationalConnection = CreateConnection(commandTimeout: 120, mutationUsage: MutationUsage.Always);
            var masterConnection = relationalConnection.CreateMasterConnection();

            Assert.NotNull(masterConnection);
            var spannerMasterConnection = Assert.IsType<SpannerRelationalConnection>(masterConnection);
            Assert.Equal(relationalConnection.ConnectionString, spannerMasterConnection.ConnectionString);
            Assert.Equal(relationalConnection.DdlExecutionStrategy, spannerMasterConnection.DdlExecutionStrategy);
            Assert.Equal(MutationUsage.Always, spannerMasterConnection.MutationUsage);
            Assert.Equal(120, spannerMasterConnection.CommandTimeout);

            var dbConnection = spannerMasterConnection.DbConnection;
            var retriableConnection = Assert.IsType<SpannerRetriableConnection>(dbConnection);
            Assert.NotNull(retriableConnection.SpannerConnection);
            Assert.False(retriableConnection.IsDisposed);

            masterConnection.Dispose();

            Assert.True(retriableConnection.IsDisposed);
        }

        [Fact]
        public async Task CreateMasterConnection_DisposesOwnedConnectionAsync()
        {
            var relationalConnection = CreateConnection();
            var masterConnection = relationalConnection.CreateMasterConnection();

            Assert.NotNull(masterConnection);
            var spannerMasterConnection = Assert.IsType<SpannerRelationalConnection>(masterConnection);
            var dbConnection = spannerMasterConnection.DbConnection;
            var retriableConnection = Assert.IsType<SpannerRetriableConnection>(dbConnection);
            Assert.NotNull(retriableConnection.SpannerConnection);
            Assert.False(retriableConnection.IsDisposed);

            await masterConnection.DisposeAsync();

            Assert.True(retriableConnection.IsDisposed);
        }

        [Fact]
        public void CreateMasterConnection_PreservesChannelCredentialsAndDdlExecutionStrategy()
        {
            var relationalConnection = CreateConnection(
                channelCredentials: ChannelCredentials.Insecure,
                ddlExecutionStrategy: DdlExecutionStrategy.StartOperation);

            var masterConnection = relationalConnection.CreateMasterConnection();

            Assert.NotNull(masterConnection);
            var spannerMasterConnection = Assert.IsType<SpannerRelationalConnection>(masterConnection);
            Assert.Equal(DdlExecutionStrategy.StartOperation, spannerMasterConnection.DdlExecutionStrategy);
            Assert.Equal(relationalConnection.ConnectionString, spannerMasterConnection.ConnectionString);
            Assert.NotNull(relationalConnection.ConnectionStringBuilder);
            Assert.NotNull(spannerMasterConnection.ConnectionStringBuilder);
            Assert.Same(relationalConnection.ConnectionStringBuilder, spannerMasterConnection.ConnectionStringBuilder);

            var dbConnection = spannerMasterConnection.DbConnection;
            var retriableConnection = Assert.IsType<SpannerRetriableConnection>(dbConnection);
            Assert.False(retriableConnection.IsDisposed);

            masterConnection.Dispose();

            Assert.True(retriableConnection.IsDisposed);
        }

        [Fact]
        public void CreateMasterConnection_PreservesRuntimeMutationsOfDdlExecutionStrategyAndCommandTimeout()
        {
            var relationalConnection = CreateConnection(
                commandTimeout: 60,
                ddlExecutionStrategy: DdlExecutionStrategy.BlockUntilCompleted);

            relationalConnection.DdlExecutionStrategy = DdlExecutionStrategy.StartOperation;
            relationalConnection.CommandTimeout = 90;

            var masterConnection = relationalConnection.CreateMasterConnection();

            Assert.NotNull(masterConnection);
            var spannerMasterConnection = Assert.IsType<SpannerRelationalConnection>(masterConnection);
            Assert.Equal(DdlExecutionStrategy.StartOperation, spannerMasterConnection.DdlExecutionStrategy);
            Assert.Equal(90, spannerMasterConnection.CommandTimeout);

            masterConnection.Dispose();
        }

        [Fact]
        public void CreateMasterConnection_PreservesRuntimeClearedCommandTimeout()
        {
            var relationalConnection = CreateConnection(commandTimeout: 60);
            relationalConnection.CommandTimeout = null;

            var masterConnection = relationalConnection.CreateMasterConnection();

            Assert.NotNull(masterConnection);
            var spannerMasterConnection = Assert.IsType<SpannerRelationalConnection>(masterConnection);
            Assert.Null(spannerMasterConnection.CommandTimeout);

            masterConnection.Dispose();
        }

        [Fact]
        public void CreateMasterConnection_WhenConfiguredWithExistingConnection_DoesNotDisposeOriginalConnection()
        {
            using var userConnection = new SpannerConnection(TestConnectionString);
            var serviceProvider = SpannerTestHelpers.Instance.CreateServiceProvider();
            var optionsBuilder = new DbContextOptionsBuilder()
                .UseInternalServiceProvider(serviceProvider)
                .UseSpanner(
                    userConnection,
                    _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false));

            var context = new DbContext(optionsBuilder.Options);
            var relationalConnection = (SpannerRelationalConnection)context.GetService<ISpannerRelationalConnection>();
            var originalDbConnection = Assert.IsType<SpannerRetriableConnection>(relationalConnection.DbConnection);

            var masterConnection = relationalConnection.CreateMasterConnection();
            var masterDbConnection = Assert.IsType<SpannerRetriableConnection>(masterConnection.DbConnection);

            Assert.NotSame(originalDbConnection, masterDbConnection);
            Assert.False(originalDbConnection.IsDisposed);
            Assert.False(masterDbConnection.IsDisposed);

            masterConnection.Dispose();

            Assert.True(masterDbConnection.IsDisposed);
            Assert.False(originalDbConnection.IsDisposed);
        }

        [Fact]
        public void CreateMasterConnection_DisposeWithoutAccessingDbConnection_Succeeds()
        {
            var relationalConnection = CreateConnection();
            var masterConnection = relationalConnection.CreateMasterConnection();

            Assert.NotNull(masterConnection);
            masterConnection.Dispose();
        }

        [Fact]
        public void CreateMasterConnection_PropagatesAllDeclaredMutableProperties()
        {
            var declaredWritableProperties = typeof(SpannerRelationalConnection)
                .GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(property => property.CanWrite && property.SetMethod != null && (property.SetMethod.IsPublic || property.SetMethod.IsAssembly))
                .Select(property => property.Name)
                .ToHashSet();

            // Properties that are mutable at runtime and must be explicitly copied in CopyRuntimeStateTo
            var expectedCopiedProperties = new HashSet<string>
            {
                nameof(SpannerRelationalConnection.DdlExecutionStrategy),
            };

            var unhandledProperties = declaredWritableProperties.Except(expectedCopiedProperties).ToList();
            Assert.True(
                unhandledProperties.Count == 0,
                $"The following writable properties on {nameof(SpannerRelationalConnection)} are not accounted for in " +
                $"{nameof(SpannerRelationalConnection.CreateMasterConnection)}: {string.Join(", ", unhandledProperties)}. " +
                $"Please ensure they are copied in CopyRuntimeStateTo / {nameof(SpannerRelationalConnection.CreateMasterConnection)} and tested.");
        }

        [Fact]
        public void CreateMasterConnection_PropagatesAllInterfaceMutableProperties()
        {
            var interfaceWritableProperties = typeof(ISpannerRelationalConnection)
                .GetInterfaces()
                .Concat(new[] { typeof(ISpannerRelationalConnection) })
                .SelectMany(interfaceType => interfaceType.GetProperties())
                .Where(property => property.CanWrite)
                .Select(property => property.Name)
                .ToHashSet();

            // Properties on ISpannerRelationalConnection or its base interfaces:
            // - DdlExecutionStrategy and CommandTimeout: copied via CopyRuntimeStateTo.
            // - ConnectionString: propagated via SpannerOptionsExtension.WithConnectionString / WithConnectionStringBuilder.
            // - DbConnection: intentionally not copied because master connection must own and create its own connection lifecycle.
            var accountedProperties = new HashSet<string>
            {
                nameof(ISpannerRelationalConnection.DdlExecutionStrategy),
                nameof(IRelationalConnection.CommandTimeout),
                nameof(IRelationalConnection.ConnectionString),
                nameof(IRelationalConnection.DbConnection),
            };

            var unhandledProperties = interfaceWritableProperties.Except(accountedProperties).ToList();
            Assert.True(
                unhandledProperties.Count == 0,
                $"The following writable properties on {nameof(ISpannerRelationalConnection)} or its base interfaces are not accounted for: " +
                $"{string.Join(", ", unhandledProperties)}.");
        }
    }
#pragma warning restore EF1001
}
