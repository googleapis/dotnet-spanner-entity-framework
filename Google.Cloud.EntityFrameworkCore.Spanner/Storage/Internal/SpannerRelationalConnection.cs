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

using System;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure.Internal;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    internal class SpannerRelationalConnection : RelationalConnection, ISpannerRelationalConnection
    {
        //Note: Wraps around a SpannerConnection.  It also sets up the log bridge for ADO.NET logs
        // to be seen in EF logs and has logic to set up a connection to the "master" db -- which in the spanner
        // world is simply a connection string that does not include a database.

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public SpannerRelationalConnection(RelationalConnectionDependencies dependencies)
            : base(dependencies)
        {
            var relationalOptions = (SpannerOptionsExtension) RelationalOptionsExtension.Extract(dependencies.ContextOptions);
            MutationUsage = relationalOptions.MutationUsage;
            ConnectionStringBuilder = relationalOptions.ConnectionStringBuilder;
        }

        //private SpannerRetriableConnection Connection => DbConnection as SpannerRetriableConnection;

        public MutationUsage MutationUsage { get; }

        private SpannerConnectionStringBuilder ConnectionStringBuilder { get; }

        /// <inheritdoc />
        protected override DbConnection CreateDbConnection()
        {
            var builder = ConnectionStringBuilder ?? new SpannerConnectionStringBuilder
            {
                ConnectionString = ConnectionString,
                SessionPoolManager = SpannerDbContextOptionsExtensions.SessionPoolManager
            };
            // if (ConnectionString!.StartsWith("Data Source=", StringComparison.OrdinalIgnoreCase))
            // {
            //     var con = new SpannerConnection(builder);
            //     return new SpannerRetriableConnection(con);
            // }
            // else
            // {
                var con = new Google.Cloud.Spanner.DataProvider.SpannerConnection();
                con.ConnectionString = builder.ConnectionString;
                return con;
            // }
        }

        /// <summary>
        /// Begins a read-only transaction on this connection.
        /// </summary>
        /// <returns>A read-only transaction that uses <see cref="TimestampBoundMode.Strong"/></returns>
        public IDbContextTransaction BeginReadOnlyTransaction() => BeginReadOnlyTransaction(TimestampBound.Strong);

        /// <summary>
        /// Begins a read-only transaction with the specified <see cref="TimestampBound"/> on this connection.
        /// </summary>
        /// <param name="timestampBound">The read timestamp to use for the transaction</param>
        /// <returns>A read-only transaction that uses the specified <see cref="TimestampBound"/></returns>
        public IDbContextTransaction BeginReadOnlyTransaction(TimestampBound timestampBound)
        {
            if (DbConnection is SpannerRetriableConnection connection)
            {
                return UseTransaction(connection.BeginReadOnlyTransaction(timestampBound));
            }
            if (DbConnection is Google.Cloud.Spanner.DataProvider.SpannerConnection spannerConnection)
            {
                return UseTransaction(spannerConnection.BeginTransaction(CreateTransactionOptions(timestampBound)));
            }
            throw new ArgumentException("Not a Spanner connection");
        }

        private static TransactionOptions CreateTransactionOptions(TimestampBound timestampBound)
        {
            TransactionOptions options = new TransactionOptions
            {
                ReadOnly = new TransactionOptions.Types.ReadOnly
                {
                    ReturnReadTimestamp = timestampBound.ReturnReadTimestamp,
                }
            };
            switch (timestampBound.Mode)
            {
                case TimestampBoundMode.Strong:
                    options.ReadOnly.Strong = true;
                    break;
                case TimestampBoundMode.ReadTimestamp:
                    options.ReadOnly.ReadTimestamp = Timestamp.FromDateTime(timestampBound.Timestamp);
                    break;
                case TimestampBoundMode.MinReadTimestamp:
                    options.ReadOnly.MinReadTimestamp = Timestamp.FromDateTime(timestampBound.Timestamp);
                    break;
                case TimestampBoundMode.ExactStaleness:
                    options.ReadOnly.ExactStaleness = Duration.FromTimeSpan(timestampBound.Staleness);
                    break;
                case TimestampBoundMode.MaxStaleness:
                    options.ReadOnly.MaxStaleness = Duration.FromTimeSpan(timestampBound.Staleness);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(timestampBound.Mode), $"unknown timestampBound mode: {timestampBound.Mode}");
            }
            return options;
        }

        /// <summary>
        /// Begins a read-only transaction on this connection.
        /// </summary>
        /// <returns>A read-only transaction that uses <see cref="TimestampBoundMode.Strong"/></returns>
        public Task<IDbContextTransaction> BeginReadOnlyTransactionAsync() => BeginReadOnlyTransactionAsync(TimestampBound.Strong);

        /// <summary>
        /// Begins a read-only transaction with the specified <see cref="TimestampBound"/> on this connection.
        /// </summary>
        /// <param name="timestampBound">The read timestamp to use for the transaction</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> cancellation token to monitor for the asynchronous operation.</param>
        /// <returns>A read-only transaction that uses the specified <see cref="TimestampBound"/></returns>
        public async Task<IDbContextTransaction> BeginReadOnlyTransactionAsync(TimestampBound timestampBound,
            CancellationToken cancellationToken = default)
        {
            if (DbConnection is SpannerRetriableConnection connection)
            {
                return await UseTransactionAsync(
                    await connection.BeginReadOnlyTransactionAsync(timestampBound, cancellationToken), cancellationToken);
            }
            if (DbConnection is Google.Cloud.Spanner.DataProvider.SpannerConnection spannerConnection)
            {
                return await UseTransactionAsync(spannerConnection.BeginTransaction(CreateTransactionOptions(timestampBound)), cancellationToken);
            }
            throw new ArgumentException("Not a Spanner connection");
        }

        /// <summary>
        /// Creates a connection to the Cloud Spanner instance that is referenced by <see cref="RelationalConnection.ConnectionString"/>.
        /// The connection is not associated with any specific database.
        /// </summary>
        public ISpannerRelationalConnection CreateMasterConnection()
        {
            // Spanner does not have anything like a master database, so we just return a new instance of a
            // RelationalConnection with the same options and dependencies. This ensures that all settings of the
            // underlying connection are carried over to the new RelationalConnection, such as credentials and host.
            var masterConn = CreateDbConnection();
            var optionsBuilder = new DbContextOptionsBuilder();
            if (masterConn is SpannerRetriableConnection spannerRetriableConnection)
            {
                optionsBuilder.UseSpanner(spannerRetriableConnection);
            }
            else if (masterConn is Google.Cloud.Spanner.DataProvider.SpannerConnection spannerConnection)
            {
                optionsBuilder.UseSpanner(spannerConnection);
            }

#pragma warning disable EF1001
            var dependencies = new RelationalConnectionDependencies(
                optionsBuilder.Options,
                Dependencies.TransactionLogger,
                Dependencies.ConnectionLogger,
                Dependencies.ConnectionStringResolver,
                Dependencies.RelationalTransactionFactory,
                Dependencies.CurrentContext,
                Dependencies.RelationalCommandBuilderFactory);
#pragma warning restore EF1001
            return new SpannerRelationalConnection(dependencies);
        }
    }
}
