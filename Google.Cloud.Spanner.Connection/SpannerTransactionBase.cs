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

using Google.Cloud.Spanner.Data;
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
    /// Base class for transactions that are used for the Cloud Spanner Entity Framework Core provider.
    /// </summary>
    public abstract class SpannerTransactionBase : DbTransaction
    {
        /// <summary>
        /// The underlying Spanner transaction. This transaction is refreshed with a new
        /// one in case the transaction is aborted by Cloud Spanner.
        /// </summary>
        protected internal SpannerTransaction SpannerTransaction { get; set; }

        protected internal bool Disposed { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (Disposed)
            {
                return;
            }
            if (disposing)
            {
                SpannerTransaction.Dispose();
            }
            Disposed = true;
            base.Dispose(disposing);
        }

        /// <inheritdoc/>
        public override IsolationLevel IsolationLevel => SpannerTransaction.IsolationLevel;

        /// <summary>
        /// Executes a <see cref="SpannerCommand"/> as an update statement and retries the entire
        /// transaction if the command fails with an Aborted error.
        /// </summary>
        /// <param name="command">The command to execute. Must be a DML or mutation command.</param>
        /// <returns>The number of affected rows.</returns>
        protected internal abstract int ExecuteNonQueryWithRetry(SpannerCommand command);

        /// <summary>
        /// Executes a <see cref="SpannerCommand"/> as an update statement and retries the entire
        /// transaction if the command fails with an Aborted error.
        /// </summary>
        /// <param name="command">The command to execute. Must be a DML or mutation command.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>The number of affected rows.</returns>
        protected internal abstract Task<int> ExecuteNonQueryWithRetryAsync(SpannerCommand command, CancellationToken cancellationToken);

        /// <summary>
        /// Executes a <see cref="SpannerBatchCommand"/> and retries the entire transaction if the
        /// command fails with an Aborted error.
        /// </summary>
        /// <param name="command">The command to execute. Must be a batch of DML statements.</param>
        /// <returns>The number of affected rows per statement.</returns>
        protected internal abstract IReadOnlyList<long> ExecuteNonQueryWithRetry(SpannerRetriableBatchCommand command);

        /// <summary>
        /// Executes a <see cref="SpannerCommand"/> as a query and returns the first column of the
        /// first row. The entire transaction is retried if the command fails with an Aborted error.
        /// </summary>
        /// <param name="command">The command to execute. Must be a query.</param>
        /// <returns>The value of the first column of the first row in the query result</returns>
        protected internal abstract object ExecuteScalarWithRetry(SpannerCommand command);

        /// <summary>
        /// Executes a <see cref="SpannerCommand"/> as a query and returns the result as a
        /// <see cref="DbDataReader"/> that will retry the entire transaction if the query or any
        /// of the results of the underlying stream of PartialResultSets returns an Aborted error.
        /// </summary>
        /// <param name="command">The command to execute. Must be a query.</param>
        /// <returns>
        /// The results of the query as a <see cref="DbDataReader"/> that will automatically retry
        /// the entire transaction if the result stream returns an Aborted error.
        /// </returns>
        protected internal abstract DbDataReader ExecuteDbDataReaderWithRetry(SpannerCommand command);
    }
}
