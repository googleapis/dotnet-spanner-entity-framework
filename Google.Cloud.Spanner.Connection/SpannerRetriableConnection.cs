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
using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.Connection
{
    /// <summary>
    /// Wrapper around a SpannerConnection that can create transactions that can be
    /// retried without the need to define the transaction as a function or action.
    /// 
    /// Transactions can only be retried if these return the same results during a
    /// retry as during the initial attempt.
    /// </summary>
    public class SpannerRetriableConnection : DbConnection
    {
        private bool _disposed;

        /// <summary>
        /// Creates a retriable connection that uses the given SpannerConnection.
        /// </summary>
        /// <param name="connection"></param>
        public SpannerRetriableConnection(SpannerConnection connection)
        {
            SpannerConnection = connection;
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }
            if (disposing)
            {
                SpannerConnection.Dispose();
            }
            _disposed = true;
            base.Dispose(disposing);
        }

        internal SpannerConnection SpannerConnection { get; private set; }

        /// <inheritdoc/>
        public override string ConnectionString { get => SpannerConnection.ConnectionString; set => SpannerConnection.ConnectionString = value; }

        /// <inheritdoc/>
        public override string Database => SpannerConnection.Database;

        /// <inheritdoc/>
        public override string DataSource => SpannerConnection.DataSource;

        /// <inheritdoc/>
        public override string ServerVersion => SpannerConnection.ServerVersion;

        /// <inheritdoc/>
        public override ConnectionState State => SpannerConnection.State;
        
        /// <summary>
        /// Sets whether this connection should create read-only transactions when <see cref="IsolationLevel.Snapshot"/>
        /// is requested. If this property has not been set to true and <see cref="BeginDbTransaction"/> is called with
        /// <see cref="IsolationLevel.Snapshot"/>, the connection will return an error, as read/write snapshots are not
        /// supported by Cloud Spanner.
        /// </summary>
        public bool CreateReadOnlyTransactionForSnapshot { get; set; }

        /// <summary>
        /// The read-only staleness to use for transactions that are started with <see cref="IsolationLevel.Snapshot"/>,
        /// and for single queries outside of a transaction.
        /// </summary>
        public TimestampBound ReadOnlyStaleness { get; set; } = TimestampBound.Strong;

        /// <summary>
        /// Begins a read-only transaction with <see cref="TimestampBoundMode.Strong"/>
        /// </summary>
        /// <returns>A new read-only transaction with <see cref="TimestampBoundMode.Strong"/></returns>
        public SpannerReadOnlyTransaction BeginReadOnlyTransaction() => BeginReadOnlyTransaction(TimestampBound.Strong);

        /// <summary>
        /// Begins a read-only transaction with the specified <see cref="TimestampBound"/>
        /// </summary>
        /// <param name="timestampBound">The read timestamp to use for the read-only transaction.</param>
        /// <returns>A new read-only transaction with the specified <see cref="TimestampBound"/></returns>
        public SpannerReadOnlyTransaction BeginReadOnlyTransaction(TimestampBound timestampBound) =>
            new SpannerReadOnlyTransaction(this, SpannerConnection.BeginReadOnlyTransaction(timestampBound));

        /// <summary>
        /// Begins a read-only transaction with <see cref="TimestampBoundMode.Strong"/>
        /// </summary>
        /// <returns>A new read-only transaction with <see cref="TimestampBoundMode.Strong"/></returns>
        public Task<SpannerReadOnlyTransaction> BeginReadOnlyTransactionAsync(CancellationToken cancellationToken = default) =>
            BeginReadOnlyTransactionAsync(TimestampBound.Strong, cancellationToken);

        /// <summary>
        /// Begins a read-only transaction with the specified <see cref="TimestampBound"/>
        /// </summary>
        /// <param name="timestampBound">The read timestamp to use for the read-only transaction.</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> cancellation token to monitor for the asynchronous operation.</param>
        /// <returns>A new read-only transaction with the specified <see cref="TimestampBound"/></returns>
        public async Task<SpannerReadOnlyTransaction> BeginReadOnlyTransactionAsync(TimestampBound timestampBound, CancellationToken cancellationToken = default) =>
            new SpannerReadOnlyTransaction(this, await SpannerConnection.BeginReadOnlyTransactionAsync(timestampBound, cancellationToken));

        /// <summary>
        /// Begins a new read/write transaction on the connection. The transaction will automatically be
        /// retried if one of the statements on the transaction is aborted by Cloud Spanner.
        /// </summary>
        /// <returns>A new read/write transaction with internal retries enabled.</returns>
        public new SpannerRetriableTransaction BeginTransaction()
            => BeginTransaction(IsolationLevel.Unspecified);

        /// <summary>
        /// Begins a new read/write transaction on the connection with the specified <see cref="IsolationLevel"/>.
        /// Cloud Spanner only supports <see cref="IsolationLevel.Serializable"/>. Trying to set a different
        /// isolation level will cause an <see cref="NotSupportedException"/>.
        /// The transaction will automatically be retried if one of the statements on the transaction
        /// is aborted by Cloud Spanner.
        /// </summary>
        /// <returns>A new read/write transaction with internal retries enabled.</returns>
        /// <exception cref="NotSupportedException"/>
        public new SpannerRetriableTransaction BeginTransaction(IsolationLevel isolationLevel)
            => BeginTransactionAsync(isolationLevel).ResultWithUnwrappedExceptions();

        /// <inheritdoc/>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (isolationLevel == IsolationLevel.Snapshot && !CreateReadOnlyTransactionForSnapshot)
            {
                throw new NotSupportedException(
                    $"{IsolationLevel.Snapshot} is only supported if CreateReadOnlyTransactionForSnapshot has been set to true. " +
                    $"{IsolationLevel.Snapshot} will then return a read-only transaction.");
            }
            if (isolationLevel == IsolationLevel.Snapshot)
            {
                return BeginReadOnlyTransaction(ReadOnlyStaleness);
            }
            return BeginTransactionAsync(isolationLevel).ResultWithUnwrappedExceptions();
        }

        /// <summary>
        /// Begins a new read/write transaction on the connection. The transaction will automatically be
        /// retried if one of the statements on the transaction is aborted by Cloud Spanner.
        /// </summary>
        /// <returns>A new read/write transaction with internal retries enabled.</returns>
        public new Task<SpannerRetriableTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
            => BeginTransactionAsync(IsolationLevel.Unspecified, cancellationToken);

        /// <summary>
        /// Begins a new read/write transaction on the connection with the specified <see cref="IsolationLevel"/>.
        /// Cloud Spanner only supports <see cref="IsolationLevel.Serializable"/>. Trying to set a different
        /// isolation level will cause an <see cref="NotSupportedException"/>.
        /// The transaction will automatically be retried if one of the statements on the transaction
        /// is aborted by Cloud Spanner.
        /// </summary>
        /// <returns>A new read/write transaction with internal retries enabled.</returns>
        /// <exception cref="NotSupportedException"/>
        public new async Task<SpannerRetriableTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        {
            if (isolationLevel != IsolationLevel.Unspecified
                && isolationLevel != IsolationLevel.Serializable)
            {
                throw new NotSupportedException(
                    $"Cloud Spanner only supports isolation levels {IsolationLevel.Serializable} and {IsolationLevel.Unspecified}.");
            }
            var spannerTransaction = await SpannerConnection.BeginTransactionAsync(cancellationToken);
            return new SpannerRetriableTransaction(
                this,
                spannerTransaction,
                SystemClock.Instance,
                SystemScheduler.Instance);
        }

        /// <summary>
        /// Creates a command that can be used for a query. It will automatically retry if the transaction aborts.
        /// </summary>
        /// <param name="sqlQueryStatement"></param>
        /// <param name="selectParameters"></param>
        /// <returns>A new command that can be used for queries</returns>
        public SpannerRetriableCommand CreateSelectCommand(string sqlQueryStatement, SpannerParameterCollection selectParameters = null) =>
            new SpannerRetriableCommand(this, SpannerConnection.CreateSelectCommand(sqlQueryStatement, selectParameters));

        /// <summary>
        /// Creates a command that can be used for a DML statement. It will automatically retry if the transaction aborts.
        /// </summary>
        /// <param name="dmlStatement"></param>
        /// <param name="dmlParameters"></param>
        /// <returns>A new command that can be used for a DML statement</returns>
        public SpannerRetriableCommand CreateDmlCommand(string dmlStatement, SpannerParameterCollection dmlParameters = null) =>
            new SpannerRetriableCommand(this, SpannerConnection.CreateDmlCommand(dmlStatement, dmlParameters));

        /// <summary>
        /// Creates a command that can be used for a batch of DML statements. It will automatically retry if the transaction aborts.
        /// </summary>
        /// <returns>A new command that can be used for a batch of DML statements</returns>
        public SpannerRetriableBatchCommand CreateBatchDmlCommand() => new SpannerRetriableBatchCommand(this);

        /// <inheritdoc/>
        protected override DbCommand CreateDbCommand() =>
            new SpannerRetriableCommand(this, new SpannerCommand { SpannerConnection = SpannerConnection });

        // TODO: Test and document mutation commands
        public SpannerRetriableCommand CreateDeleteCommand(
            string databaseTable,
            SpannerParameterCollection primaryKeys = null) =>
            new SpannerRetriableCommand(this, SpannerConnection.CreateDeleteCommand(databaseTable, primaryKeys));

        public SpannerRetriableCommand CreateInsertCommand(
            string databaseTable,
            SpannerParameterCollection insertedColumns = null) =>
            new SpannerRetriableCommand(this, SpannerConnection.CreateInsertCommand(databaseTable, insertedColumns));

        public SpannerRetriableCommand CreateInsertOrUpdateCommand(
            string databaseTable,
            SpannerParameterCollection insertUpdateColumns = null) =>
            new SpannerRetriableCommand(this, SpannerConnection.CreateInsertOrUpdateCommand(databaseTable, insertUpdateColumns));

        public SpannerRetriableCommand CreateUpdateCommand(string databaseTable, SpannerParameterCollection updateColumns = null) =>
            new SpannerRetriableCommand(this, SpannerConnection.CreateUpdateCommand(databaseTable, updateColumns));

        public SpannerRetriableCommand CreateDdlCommand(
            string ddlStatement, params string[] extraDdlStatements) =>
            new SpannerRetriableCommand(this, SpannerConnection.CreateDdlCommand(ddlStatement, extraDdlStatements));

        /// <inheritdoc/>
        public override void ChangeDatabase(string databaseName) => SpannerConnection.ChangeDatabase(databaseName);

        /// <inheritdoc/>
        public override void Close()
        {
            ReadOnlyStaleness = TimestampBound.Strong;
            SpannerConnection.Close();
        }

        /// <inheritdoc/>
        public override void Open() => SpannerConnection.Open();
    }
}
