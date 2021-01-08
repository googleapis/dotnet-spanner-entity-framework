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
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage
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
        public SpannerRetriableConnection(SpannerConnection connection)
        {
            SpannerConnection = connection;
        }

        internal SpannerConnection SpannerConnection { get; private set; }

        public override string ConnectionString { get => SpannerConnection.ConnectionString; set => SpannerConnection.ConnectionString = value; }

        public override string Database => SpannerConnection.Database;

        public override string DataSource => SpannerConnection.DataSource;

        public override string ServerVersion => SpannerConnection.ServerVersion;

        public override ConnectionState State => SpannerConnection.State;

        public SpannerReadOnlyTransaction BeginReadOnlyTransaction() => BeginReadOnlyTransaction(TimestampBound.Strong);

        public SpannerReadOnlyTransaction BeginReadOnlyTransaction(TimestampBound timestampBound) =>
            new SpannerReadOnlyTransaction(this, SpannerConnection.BeginReadOnlyTransaction(timestampBound));

        public Task<SpannerReadOnlyTransaction> BeginReadOnlyTransactionAsync(CancellationToken cancellationToken = default) =>
            BeginReadOnlyTransactionAsync(TimestampBound.Strong, cancellationToken);

        public async Task<SpannerReadOnlyTransaction> BeginReadOnlyTransactionAsync(TimestampBound timestampBound, CancellationToken cancellationToken = default) =>
            new SpannerReadOnlyTransaction(this, await SpannerConnection.BeginReadOnlyTransactionAsync(timestampBound, cancellationToken));

        public new SpannerRetriableTransaction BeginTransaction()
            => BeginTransaction(IsolationLevel.Unspecified);

        public new SpannerRetriableTransaction BeginTransaction(IsolationLevel isolationLevel)
            => BeginTransactionAsync(isolationLevel).ResultWithUnwrappedExceptions();

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => BeginTransactionAsync(isolationLevel).ResultWithUnwrappedExceptions();

        public Task<SpannerRetriableTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
            => BeginTransactionAsync(IsolationLevel.Unspecified, cancellationToken);

        public async Task<SpannerRetriableTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
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

        public SpannerRetriableCommand CreateSelectCommand(string sqlQueryStatement, SpannerParameterCollection selectParameters = null) =>
            new SpannerRetriableCommand(this, SpannerConnection.CreateSelectCommand(sqlQueryStatement, selectParameters));

        public SpannerRetriableCommand CreateDmlCommand(string dmlStatement, SpannerParameterCollection dmlParameters = null) =>
            new SpannerRetriableCommand(this, SpannerConnection.CreateDmlCommand(dmlStatement, dmlParameters));

        public SpannerRetriableBatchCommand CreateBatchDmlCommand() => new SpannerRetriableBatchCommand(this);

        protected override DbCommand CreateDbCommand() =>
            new SpannerRetriableCommand(this, new SpannerCommand { SpannerConnection = SpannerConnection });

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

        public override void ChangeDatabase(string databaseName) => SpannerConnection.ChangeDatabase(databaseName);

        public override void Close() => SpannerConnection.Close();

        public override void Open() => SpannerConnection.Open();

    }
}
