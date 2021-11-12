// Copyright 2021, Google Inc. All rights reserved.
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
using Google.Cloud.Spanner.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.Connection
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// Multi-use read-only transaction for a Cloud Spanner database.
    /// </summary>
    public class SpannerReadOnlyTransaction : SpannerTransactionBase
    {
        /// <summary>
        /// Creates a wrapper around an existing Spanner read-only transaction that can be used with EFCore.
        /// Read-only transactions are never aborted by Cloud Spanner and no additional retry handling is
        /// needed for this type of transaction.
        /// </summary>
        /// <param name="connection">The connection associated with the transaction</param>
        /// <param name="spannerTransaction">The underlying Spanner transaction. This transaction must be marked as read-only</param>
        public SpannerReadOnlyTransaction(SpannerRetriableConnection connection, SpannerTransaction spannerTransaction)
        {
            GaxPreconditions.CheckNotNull(spannerTransaction, nameof(spannerTransaction));
            GaxPreconditions.CheckArgument(spannerTransaction.Mode == TransactionMode.ReadOnly, nameof(spannerTransaction), "Must be a read-only transaction");
            DbConnection = GaxPreconditions.CheckNotNull(connection, nameof(connection));
            SpannerTransaction = spannerTransaction;
        }

        /// <inheritdoc/>
        protected override DbConnection DbConnection { get; }

        /// <summary>
        /// Spanner read-only transactions cannot be committed, but calling this method will dispose the transaction
        /// and make it possible to start a new transaction on the database.
        /// </summary>
        public override void Commit() => Dispose(true);

        /// <summary>
        /// Spanner read-only transactions cannot be rollbacked, but calling this method will dispose the transaction
        /// and make it possible to start a new transaction on the database.
        /// </summary>
        public override void Rollback() => Dispose(true);

        /// <summary>
        /// Read-only transactions cannot execute non-query statements. Calling this method will throw an <see cref="InvalidOperationException"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
        protected internal override int ExecuteNonQueryWithRetry(SpannerCommand command) =>
            throw new InvalidOperationException("Non-query operations are not allowed on a read-only transaction");

        /// <summary>
        /// Read-only transactions cannot execute non-query statements. Calling this method will throw an <see cref="InvalidOperationException"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
        protected internal override Task<int> ExecuteNonQueryWithRetryAsync(SpannerCommand command, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Non-query operations are not allowed on a read-only transaction");

        /// <summary>
        /// Read-only transactions cannot execute non-query statements. Calling this method will throw an <see cref="InvalidOperationException"/>.
        /// </summary>
        /// <exception cref="InvalidOperationException"></exception>
        protected internal override IReadOnlyList<long> ExecuteNonQueryWithRetry(SpannerRetriableBatchCommand command) =>
            throw new InvalidOperationException("Non-query operations are not allowed on a read-only transaction");

        /// <inheritdoc/>
        protected internal override object ExecuteScalarWithRetry(SpannerCommand command)
        {
            GaxPreconditions.CheckState(!Disposed, "This transaction has been disposed");
            command.Transaction = SpannerTransaction;
            return command.ExecuteScalar();
        }

        /// <inheritdoc/>
        protected internal override DbDataReader ExecuteDbDataReaderWithRetry(SpannerCommand command)
        {
            GaxPreconditions.CheckState(!Disposed, "This transaction has been disposed");
            command.Transaction = SpannerTransaction;
            return command.ExecuteReader();
        }
    }
}
