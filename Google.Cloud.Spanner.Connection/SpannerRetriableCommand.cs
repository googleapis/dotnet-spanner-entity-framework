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
    /// This is internal functionality and not intended for public use.
    /// <see cref="DbCommand"/> implementation for Cloud Spanner that can be retried if the underlying
    /// Spanner transaction is aborted.
    /// </summary>
    public class SpannerRetriableCommand : DbCommand
    {
        private SpannerRetriableConnection _connection;
        private readonly SpannerCommand _spannerCommand;
        private SpannerTransactionBase _transaction;

        public SpannerRetriableCommand(SpannerCommand spannerCommand) =>
            _spannerCommand = spannerCommand;

        internal SpannerRetriableCommand(SpannerRetriableConnection connection, SpannerCommand spannerCommand)
        {
            _connection = connection;
            _spannerCommand = (SpannerCommand)GaxPreconditions.CheckNotNull(spannerCommand, nameof(spannerCommand)).Clone();
        }

        public object Clone() => new SpannerRetriableCommand(_connection, _spannerCommand.Clone() as SpannerCommand)
        {
            Transaction = _transaction,
        };

        public override string CommandText { get => _spannerCommand.CommandText; set => _spannerCommand.CommandText = value; }
        public override int CommandTimeout { get => _spannerCommand.CommandTimeout; set => _spannerCommand.CommandTimeout = value; }
        public override CommandType CommandType { get => _spannerCommand.CommandType; set => _spannerCommand.CommandType = value; }
        public override bool DesignTimeVisible { get => _spannerCommand.DesignTimeVisible; set => _spannerCommand.DesignTimeVisible = value; }
        public override UpdateRowSource UpdatedRowSource { get => _spannerCommand.UpdatedRowSource; set => _spannerCommand.UpdatedRowSource = value; }
        protected override DbConnection DbConnection
        {
            get => _connection;
            set
            {
                if (!(value is SpannerRetriableConnection retriableConnection))
                {
                    throw new ArgumentException( "The connection must be a SpannerRetriableConnection", nameof(value));
                }
                _connection = retriableConnection;
                _spannerCommand.Connection = retriableConnection.SpannerConnection;
            } 
        }

        protected override DbTransaction DbTransaction
        {
            get => _transaction;
            set => _transaction = (SpannerTransactionBase)value;
        }
        
        public TimestampBound TimestampBound { get; set; }
        
        internal SpannerCommand SpannerCommand => _spannerCommand;

        protected override DbParameterCollection DbParameterCollection => _spannerCommand.Parameters;

        protected override DbParameter CreateDbParameter() => new SpannerParameter();

        public override void Cancel() => _spannerCommand.Cancel();

        public override int ExecuteNonQuery() =>
            _transaction?.ExecuteNonQueryWithRetry(_spannerCommand) ?? ExecuteNonQueryWithRetryAsync(_spannerCommand).ResultWithUnwrappedExceptions();

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) =>
            _transaction == null
            ? ExecuteNonQueryWithRetryAsync(_spannerCommand, cancellationToken)
            : _transaction.ExecuteNonQueryWithRetryAsync(_spannerCommand, cancellationToken);

        /// <summary>
        /// Wraps a DML command in a Spanner retriable transaction to retry Aborted errors.
        /// </summary>
        private async Task<int> ExecuteNonQueryWithRetryAsync(SpannerCommand spannerCommand, CancellationToken cancellationToken = default)
        {
            var builder = SpannerCommandTextBuilder.FromCommandText(spannerCommand.CommandText);
            if (builder.SpannerCommandType == SpannerCommandType.Ddl)
            {
                return await spannerCommand.ExecuteNonQueryAsync(cancellationToken);
            }
            return await _connection.SpannerConnection.RunWithRetriableTransactionAsync(async transaction =>
            {
                spannerCommand.Transaction = transaction;
                return await spannerCommand.ExecuteNonQueryAsync(cancellationToken);
            }, cancellationToken);
        }

        public override object ExecuteScalar() =>
            _transaction == null
            // These don't need retry protection as the ephemeral transaction used by the client library is a read-only transaction.
            ? _spannerCommand.ExecuteScalar()
            : _transaction.ExecuteScalarWithRetry(_spannerCommand);

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_transaction != null)
            {
                return _transaction.ExecuteDbDataReaderWithRetry(_spannerCommand);
            }
            // These don't need retry protection as the ephemeral transaction used by the client library is a read-only transaction.
            if (TimestampBound != null || _connection.ReadOnlyStaleness != null && _connection.ReadOnlyStaleness.Mode != TimestampBoundMode.Strong)
            {
                return _spannerCommand.ExecuteReaderAsync(TimestampBound ?? _connection.ReadOnlyStaleness)
                    .ResultWithUnwrappedExceptions();
            }
            return _spannerCommand.ExecuteReader();
        }

        public override void Prepare() => _spannerCommand.Prepare();
    }
}
