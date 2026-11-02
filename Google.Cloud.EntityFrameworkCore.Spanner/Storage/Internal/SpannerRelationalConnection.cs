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
using System.Data;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure.Internal;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;

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
            var relationalOptions = (SpannerOptionsExtension)RelationalOptionsExtension.Extract(dependencies.ContextOptions);
            DdlExecutionStrategy = relationalOptions.DdlExecutionStrategy;
            MutationUsage = relationalOptions.MutationUsage;
            ConnectionStringBuilder = relationalOptions.ConnectionStringBuilder;
        }

        private SpannerRetriableConnection Connection => DbConnection as SpannerRetriableConnection;

        /// <summary>
        /// Implements ISpannerRelationalConnection.DdlExecutionStrategy
        /// </summary>
        public DdlExecutionStrategy DdlExecutionStrategy { get; set; }

        public MutationUsage MutationUsage { get; }

        internal SpannerConnectionStringBuilder ConnectionStringBuilder { get; }

        private void CopyRuntimeStateTo(SpannerRelationalConnection target)
        {
            target.DdlExecutionStrategy = DdlExecutionStrategy;
            target.CommandTimeout = CommandTimeout;
        }

        /// <inheritdoc />
        protected override DbConnection CreateDbConnection()
        {
            var builder = ConnectionStringBuilder ?? new SpannerConnectionStringBuilder
            {
                ConnectionString = ConnectionString,
                SessionPoolManager = SpannerDbContextOptionsExtensions.SessionPoolManager
            };
            var con = new SpannerConnection(builder);
            return new SpannerRetriableConnection(con);
        }

        /// <summary>
        /// Begins a read/write transaction on this connection with the given transaction tag.
        /// </summary>
        /// <param name="tag">The transaction tag to use for the transaction</param>
        /// <returns>A read/write transaction that uses the given tag</returns>
        public IDbContextTransaction BeginTransaction(string tag) => BeginTransaction(IsolationLevel.Unspecified, tag);

        /// <inheritdoc />
        public override IDbContextTransaction BeginTransaction(IsolationLevel isolationLevel)
            => BeginTransaction(isolationLevel, tag: null);

        /// <summary>
        /// Begins a read/write transaction on this connection with the given isolation level and transaction tag.
        /// </summary>
        /// <param name="isolationLevel">The isolation level to use for the transaction</param>
        /// <param name="tag">The transaction tag to use for the transaction</param>
        /// <returns>A read/write transaction that uses the given isolation level and tag</returns>
        public IDbContextTransaction BeginTransaction(IsolationLevel isolationLevel, string tag)
            => BeginCustomTransaction(isolationLevel, () => Connection.BeginTransaction(isolationLevel, tag));

        private IDbContextTransaction CreateRelationalTransaction(DbTransaction transaction, Guid transactionId, bool transactionOwned)
            => CurrentTransaction
                = Dependencies.RelationalTransactionFactory.Create(
                    this,
                    transaction,
                    transactionId,
                    Dependencies.TransactionLogger,
                    transactionOwned: transactionOwned);

        /// <summary>
        /// Begins a read/write transaction on this connection with the given transaction tag.
        /// </summary>
        /// <param name="tag">The transaction tag to use for the transaction</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> cancellation token to monitor for the asynchronous operation.</param>
        /// <returns>A read/write transaction that uses the given tag</returns>
        public Task<IDbContextTransaction> BeginTransactionAsync(string tag, CancellationToken cancellationToken = default)
            => BeginTransactionAsync(IsolationLevel.Unspecified, tag, cancellationToken);

        /// <inheritdoc />
        public override Task<IDbContextTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
            => BeginTransactionAsync(isolationLevel, tag: null, cancellationToken);

        /// <summary>
        /// Begins a read/write transaction on this connection with the given isolation level and transaction tag.
        /// </summary>
        /// <param name="isolationLevel">The isolation level to use for the transaction</param>
        /// <param name="tag">The transaction tag to use for the transaction</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> cancellation token to monitor for the asynchronous operation.</param>
        /// <returns>A read/write transaction that uses the given isolation level and tag</returns>
        public Task<IDbContextTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, string tag, CancellationToken cancellationToken = default)
            => BeginCustomTransactionAsync(isolationLevel, token => Connection.BeginTransactionAsync(isolationLevel, tag, token), cancellationToken);

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
            GaxPreconditions.CheckNotNull(timestampBound, nameof(timestampBound));
            return BeginCustomTransaction(IsolationLevel.Unspecified, () => Connection.BeginReadOnlyTransaction(timestampBound));
        }

        /// <summary>
        /// Begins a read-only transaction on this connection.
        /// </summary>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> cancellation token to monitor for the asynchronous operation.</param>
        /// <returns>A read-only transaction that uses <see cref="TimestampBoundMode.Strong"/></returns>
        public Task<IDbContextTransaction> BeginReadOnlyTransactionAsync(CancellationToken cancellationToken = default)
            => BeginReadOnlyTransactionAsync(TimestampBound.Strong, cancellationToken);

        /// <summary>
        /// Begins a read-only transaction with the specified <see cref="TimestampBound"/> on this connection.
        /// </summary>
        /// <param name="timestampBound">The read timestamp to use for the transaction</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> cancellation token to monitor for the asynchronous operation.</param>
        /// <returns>A read-only transaction that uses the specified <see cref="TimestampBound"/></returns>
        public Task<IDbContextTransaction> BeginReadOnlyTransactionAsync(TimestampBound timestampBound, CancellationToken cancellationToken = default)
        {
            GaxPreconditions.CheckNotNull(timestampBound, nameof(timestampBound));
            return BeginCustomTransactionAsync(IsolationLevel.Unspecified, token => Connection.BeginReadOnlyTransactionAsync(timestampBound, token), cancellationToken);
        }

        private IDbContextTransaction BeginCustomTransaction(
            IsolationLevel isolationLevel,
            Func<DbTransaction> beginTransactionAction)
        {
            EnsureNoTransactions();
            Open();
            DbTransaction transaction = null;
            try
            {
                EnsureNoTransactions();
                var transactionId = Guid.NewGuid();
                var startTime = DateTimeOffset.UtcNow;
                var stopwatch = Stopwatch.StartNew();
                var interceptionResult = Dependencies.TransactionLogger.TransactionStarting(
                    this, isolationLevel, transactionId, startTime);
                transaction = interceptionResult.HasResult
                    ? interceptionResult.Result
                    : beginTransactionAction();
                transaction = Dependencies.TransactionLogger.TransactionStarted(
                    this, transaction, transactionId, startTime, stopwatch.Elapsed);
                return CreateRelationalTransaction(transaction, transactionId, transactionOwned: true);
            }
            catch
            {
                try
                {
                    transaction?.Dispose();
                }
                catch
                {
                    // Suppress secondary exception to ensure Close() is reached.
                }
                Close();
                throw;
            }
        }

        private async Task<IDbContextTransaction> BeginCustomTransactionAsync<TTransaction>(
            IsolationLevel isolationLevel,
            Func<CancellationToken, Task<TTransaction>> beginTransactionActionAsync,
            CancellationToken cancellationToken)
            where TTransaction : DbTransaction
        {
            EnsureNoTransactions();
            await OpenAsync(cancellationToken).ConfigureAwait(false);
            DbTransaction transaction = null;
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                EnsureNoTransactions();
                var transactionId = Guid.NewGuid();
                var startTime = DateTimeOffset.UtcNow;
                var stopwatch = Stopwatch.StartNew();
                var interceptionResult = await Dependencies.TransactionLogger.TransactionStartingAsync(
                    this, isolationLevel, transactionId, startTime, cancellationToken).ConfigureAwait(false);
                if (interceptionResult.HasResult)
                {
                    transaction = interceptionResult.Result;
                    cancellationToken.ThrowIfCancellationRequested();
                }
                else
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    transaction = await beginTransactionActionAsync(cancellationToken).ConfigureAwait(false);
                }
                transaction = await Dependencies.TransactionLogger.TransactionStartedAsync(
                    this, transaction, transactionId, startTime, stopwatch.Elapsed, cancellationToken).ConfigureAwait(false);
                return CreateRelationalTransaction(transaction, transactionId, transactionOwned: true);
            }
            catch
            {
                if (transaction != null)
                {
                    try
                    {
                        await transaction.DisposeAsync().ConfigureAwait(false);
                    }
                    catch
                    {
                        // Suppress secondary exception to ensure CloseAsync() is reached.
                    }
                }
                await CloseAsync().ConfigureAwait(false);
                throw;
            }
        }

        private void EnsureNoTransactions()
        {
            if (CurrentTransaction != null)
            {
                throw new InvalidOperationException(RelationalStrings.TransactionAlreadyStarted);
            }
            if (CurrentAmbientTransaction != null)
            {
                throw new InvalidOperationException(RelationalStrings.ConflictingAmbientTransaction);
            }
            if (EnlistedTransaction != null)
            {
                throw new InvalidOperationException(RelationalStrings.ConflictingEnlistedTransaction);
            }
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
            var extension = (SpannerOptionsExtension)(Dependencies.ContextOptions.FindExtension<SpannerOptionsExtension>() ?? new SpannerOptionsExtension())
                .WithConnection(null);
            extension = extension.WithDdlExecutionStrategy(DdlExecutionStrategy);

            extension = ConnectionStringBuilder != null
                ? extension.WithConnectionStringBuilder(ConnectionStringBuilder)
                : (SpannerOptionsExtension)extension.WithConnectionString(ConnectionString);

            var options = ((DbContextOptions)Dependencies.ContextOptions).WithExtension(extension);

#pragma warning disable EF1001
            var dependencies = Dependencies with { ContextOptions = options };
#pragma warning restore EF1001
            var masterConnection = new SpannerRelationalConnection(dependencies);
            CopyRuntimeStateTo(masterConnection);
            return masterConnection;
        }
    }
}
