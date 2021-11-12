// Copyright 2021 Google LLC
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

using Google.Api.Gax;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.Connection
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// 
    /// Represents a SQL transaction to be made in a Spanner database.
    /// A transaction in Cloud Spanner is a set of reads and writes that execute
    /// atomically at a single logical point in time across columns, rows, and
    /// tables in a database.
    /// 
    /// This transaction will automatically be retried if it is aborted by the
    /// Cloud Spanner backend. Retrying aborted transactions is handled as follows:
    /// 
    /// 1. All statements that are executed during the transaction and their results are recorded.
    /// 2. For DML and Batch DML statements that means keeping track of the statement(s) and the returned
    ///    update counts. In case the statement returns an error, the error is recorded.
    /// 3. For SELECT statements this means keeping track of the statement and a running checksum of the
    ///    results that have been returned to the client application.
    /// 4. If the transaction is aborted by Cloud Spanner, a retry is initiated by starting a new read/write
    ///    transaction and executing all statements from the initial transaction on the new transaction. If
    ///    these results are equal to the original results, the retry is deemed successful and the transaction
    ///    may proceed using the fresh underlying transaction.
    /// </summary>
    public sealed class SpannerRetriableTransaction : SpannerTransactionBase
    {
        internal static bool SpannerExceptionsEqualForRetry(SpannerException e1, SpannerException e2)
        {
            // Quick return for the most common case.
            if (e1 == null && e2 == null)
            {
                return true;
            }
            if (!Equals(e1?.ErrorCode, e2?.ErrorCode))
            {
                return false;
            }
            if (!Equals(e1.Message, e2.Message))
            {
                return false;
            }
            if (e1.InnerException?.GetType() != e2.InnerException?.GetType())
            {
                return false;
            }
            if (e1.InnerException is RpcException rpcException1 && e2.InnerException is RpcException rpcException2)
            {
                var status1 = rpcException1.Status;
                var status2 = rpcException2.Status;
                if (!(Equals(status1.StatusCode, status2.StatusCode) && Equals(status1.Detail, status2.Detail)))
                {
                    return false;
                }
            }
            return true;
        }

        private const int MaxTimeoutSeconds = int.MaxValue / 1000; // Max is Int32.MaxValue milliseconds.
        private readonly IClock _clock;
        private readonly IScheduler _scheduler;
        private readonly RetriableTransactionOptions _options;
        private readonly List<IRetriableStatement> _retriableStatements = new List<IRetriableStatement>();
        public int RetryCount { get; private set; }

        internal SpannerRetriableTransaction(
            SpannerRetriableConnection connection,
            SpannerTransaction spannerTransaction,
            IClock clock,
            IScheduler scheduler,
            RetriableTransactionOptions options = null)
        {
            Connection = connection;
            SpannerTransaction = spannerTransaction;
            _clock = GaxPreconditions.CheckNotNull(clock, nameof(clock));
            _scheduler = GaxPreconditions.CheckNotNull(scheduler, nameof(scheduler));
            _options = options ?? RetriableTransactionOptions.CreateDefault();
        }

        /// <summary>
        /// Enables/disables internal retries in case of an aborted transaction.
        /// Internal retries are enabled by default.
        /// </summary>
        public bool EnableInternalRetries { get; set; } = true;

        /// <summary>
        /// Sets the maximum number of internal retries that the transaction should
        /// execute before giving up. Defaults to 100.
        /// </summary>
        public int MaxInternalRetryCount { get; set; } = 100;

        /// <summary>
        /// The TransactionId of the underlying Spanner transaction. This id can change
        /// during the lifetime of this transaction, as the underlying Spanner transaction
        /// will be replaced with a new one in case of a retry.
        /// </summary>
        public TransactionId TransactionId => SpannerTransaction.TransactionId;

        private DateTime? _commitTimestamp;

        /// <summary>
        /// The commit timestamp of this transaction. Only available after the transaction has committed.
        /// </summary>
        /// <exception cref="InvalidOperationException">If the transaction has not committed</exception>
        public DateTime CommitTimestamp
        {
            get
            {
                GaxPreconditions.CheckState(_commitTimestamp != null, "Transaction has not committed");
                return (DateTime)_commitTimestamp!;
            }
        }

        /// <see cref="SpannerTransaction.Rollback"/>
        public override void Rollback() => SpannerTransaction.Rollback();

        internal new SpannerRetriableConnection Connection { get; private set; }

        /// <inheritdoc/>
        protected override DbConnection DbConnection => Connection;

        /// <inheritdoc/>
        public override IsolationLevel IsolationLevel => SpannerTransaction.IsolationLevel;

        protected internal override int ExecuteNonQueryWithRetry(SpannerCommand command)
            => Task.Run(() => ExecuteNonQueryWithRetryAsync(command, CancellationToken.None)).ResultWithUnwrappedExceptions();

        protected internal override async Task<int> ExecuteNonQueryWithRetryAsync(SpannerCommand command, CancellationToken cancellationToken)
        {
            while (true)
            {
                command.Transaction = SpannerTransaction;
                try
                {
                    var res = await command.ExecuteNonQueryAsync(cancellationToken);
                    _retriableStatements.Add(new RetriableDmlStatement(command, res));
                    return res;
                }
                catch (SpannerException e) when (e.ErrorCode == ErrorCode.Aborted)
                {
                    await RetryAsync(e, cancellationToken);
                }
                catch (SpannerException e)
                {
                    _retriableStatements.Add(new FailedDmlStatement(command, e));
                    throw;
                }
            }
        }

        protected internal override IReadOnlyList<long> ExecuteNonQueryWithRetry(SpannerRetriableBatchCommand command)
            => Task.Run(() => ExecuteNonQueryWithRetryAsync(command, CancellationToken.None)).ResultWithUnwrappedExceptions();

        internal async Task<IReadOnlyList<long>> ExecuteNonQueryWithRetryAsync(SpannerRetriableBatchCommand command, CancellationToken cancellationToken = default)
        {
            while (true)
            {
                var spannerCommand = command.CreateSpannerBatchCommand();
                try
                {
                    IReadOnlyList<long> res = await spannerCommand.ExecuteNonQueryAsync(cancellationToken);
                    _retriableStatements.Add(new RetriableBatchDmlStatement(command, res));
                    return res;
                }
                catch (SpannerException e) when (e.ErrorCode == ErrorCode.Aborted)
                {
                    await RetryAsync(e, cancellationToken);
                }
                catch (SpannerException e)
                {
                    _retriableStatements.Add(new FailedBatchDmlStatement(command, e));
                    throw;
                }
            }
        }

        protected internal override object ExecuteScalarWithRetry(SpannerCommand command)
            => Task.Run(() => ExecuteScalarWithRetryAsync(command, CancellationToken.None)).ResultWithUnwrappedExceptions();

        internal async Task<object> ExecuteScalarWithRetryAsync(SpannerCommand command, CancellationToken cancellationToken)
        {
            using var reader = await ExecuteDbDataReaderWithRetryAsync(command, cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                return reader.GetValue(0);
            }

            return null;
        }

        protected internal override DbDataReader ExecuteDbDataReaderWithRetry(SpannerCommand command)
            => Task.Run(() => ExecuteDbDataReaderWithRetryAsync(command, CancellationToken.None)).ResultWithUnwrappedExceptions();

        internal async Task<SpannerDataReaderWithChecksum> ExecuteDbDataReaderWithRetryAsync(SpannerCommand command, CancellationToken cancellationToken)
        {
            // This method does not need a retry loop as it is not actually executing the query. Instead,
            // that will be deferred until the first call to DbDataReader.Read().
            command.Transaction = SpannerTransaction;
            var spannerReader = await command.ExecuteReaderAsync(cancellationToken);
            var checksumReader = new SpannerDataReaderWithChecksum(this, spannerReader, command);
            _retriableStatements.Add(checksumReader);
            return checksumReader;
        }

        internal void Retry(SpannerException abortedException, int timeoutSeconds = MaxTimeoutSeconds)
            => RetryAsync(abortedException, CancellationToken.None, timeoutSeconds).WaitWithUnwrappedExceptions();

        internal async Task RetryAsync(SpannerException abortedException, CancellationToken cancellationToken, int timeoutSeconds = MaxTimeoutSeconds)
        {
            if (!EnableInternalRetries)
            {
                throw abortedException;
            }
            DateTime? overallDeadline = _options.CalculateDeadline(_clock);
            TimeSpan retryDelay = _options.InitialDelay;
            // If there's a recommended retry delay specified on the exception
            // we should respect it.
            retryDelay = abortedException.RecommendedRetryDelay ?? _options.Jitter(retryDelay);
            while (true)
            {
                if (RetryCount >= MaxInternalRetryCount)
                {
                    throw new SpannerException(ErrorCode.Aborted, "Transaction was aborted because it aborted and retried too many times");
                }

                DateTime expectedRetryTime = _clock.GetCurrentDateTimeUtc() + retryDelay;
                if (expectedRetryTime > overallDeadline)
                {
                    throw new SpannerException(ErrorCode.Aborted, "Transaction was aborted because it timed out while retrying");
                }
                await _scheduler.Delay(retryDelay, cancellationToken).ConfigureAwait(false);

                // TODO: Preferably the Spanner client library should have some 'reset' option on an existing
                // transaction instead of having to begin a new transaction. This will potentially use a transaction
                // on a different session, while the recommended behavior for a retry is to use the same session as
                // the original attempt.
                SpannerTransaction.Dispose();
                SpannerTransaction = await Connection.SpannerConnection.BeginTransactionAsync(cancellationToken);
                RetryCount++;
                try
                {
                    foreach (IRetriableStatement statement in _retriableStatements)
                    {
                        await statement.RetryAsync(this, cancellationToken, timeoutSeconds);
                    }
                    break;
                }
                catch (SpannerAbortedDueToConcurrentModificationException)
                {
                    // Retry failed because of a concurrent modification.
                    throw;
                }
                catch (SpannerException e) when (e.ErrorCode == ErrorCode.Aborted)
                {
                    // Ignore and retry.
                    retryDelay = e.RecommendedRetryDelay ?? _options.Jitter(_options.NextDelay(retryDelay));
                }
            }
        }

        /// <summary>
        /// Commits the database transaction asynchronously.
        /// </summary>
        /// <param name="cancellationToken">A cancellation token used for this task.</param>
        /// <returns>Returns the UTC timestamp when the data was written to the database.</returns>
        public new async Task<DateTime> CommitAsync(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                try
                {
                    _commitTimestamp = await SpannerTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    return (DateTime)_commitTimestamp;
                }
                catch (SpannerException e) when (e.ErrorCode == ErrorCode.Aborted)
                {
                    await RetryAsync(e, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        /// <inheritdoc />
        public override void Commit() => CommitAsync(CancellationToken.None).WaitWithUnwrappedExceptions();
    }
}