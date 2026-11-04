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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using V1 = Google.Cloud.Spanner.V1;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class RetriableStatementsTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;
        private readonly SessionPoolManager _manager;

        public RetriableStatementsTests(SpannerMockServerFixture service)
        {
            _fixture = service;
            _fixture.SpannerMock.Reset();
            
            var options = new V1.SessionPoolOptions();
            options.MinimumPooledSessions = 4;
            options.MaximumActiveSessions = 8;
            options.WaitOnResourcesExhausted = V1.ResourcesExhaustedBehavior.Fail;
            _manager = SessionPoolManager.Create(options);
        }

        private SpannerRetriableConnection CreateConnection()
        {
            var connectionString = $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";
            var builder = new SpannerConnectionStringBuilder(connectionString, ChannelCredentials.Insecure);
            builder.SessionPoolManager = _manager;
            return new SpannerRetriableConnection(new SpannerConnection(builder));
        }

        [Fact]
        public void RetriableDmlStatement_SyncRetry_Succeeds()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
            
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();
            
            var command = connection.SpannerConnection.CreateDmlCommand(sql);
            command.Transaction = transaction.SpannerTransaction;
            
            var updateCount = command.ExecuteNonQuery();
            var statement = new RetriableDmlStatement(command, updateCount);
            
            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)statement).Retry(retryTransaction, 60);
        }

        [Fact]
        public void RetriableDmlStatement_SyncRetry_FailsOnModifiedCount()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
            
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();
            
            var command = connection.SpannerConnection.CreateDmlCommand(sql);
            command.Transaction = transaction.SpannerTransaction;
            
            var updateCount = command.ExecuteNonQuery();
            var statement = new RetriableDmlStatement(command, updateCount);
            
            // Change DB response for the retry attempt
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(2));
            
            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public void FailedDmlStatement_SyncRetry_Succeeds()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));
            
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();
            
            var command = connection.SpannerConnection.CreateDmlCommand(sql);
            command.Transaction = transaction.SpannerTransaction;
            
            var caughtException = Assert.Throws<SpannerException>(() => command.ExecuteNonQuery());
            
            var statement = new FailedDmlStatement(command, caughtException);
            
            using var retryTransaction = connection.BeginTransaction();
            // During retry, the same exception is thrown, which the statement translates into a success/return
            ((IRetriableStatement)statement).Retry(retryTransaction, 60);
        }

        [Fact]
        public void FailedDmlStatement_SyncRetry_FailsOnDifferentError()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));
            
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();
            
            var command = connection.SpannerConnection.CreateDmlCommand(sql);
            command.Transaction = transaction.SpannerTransaction;
            
            var caughtException = Assert.Throws<SpannerException>(() => command.ExecuteNonQuery());
            
            var statement = new FailedDmlStatement(command, caughtException);
            
            // Change DB response to a different error for the retry
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.Internal, "Internal error"))));
            
            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public void RetriableBatchDmlStatement_SyncRetry_Succeeds()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));
            
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();
            
            var batchCmd = connection.CreateBatchDmlCommand();
            batchCmd.Transaction = transaction;
            batchCmd.Add(sql1);
            batchCmd.Add(sql2);
            
            var updateCounts = batchCmd.ExecuteNonQuery();
            Assert.Equal(new List<long> { 1, 2 }, updateCounts);
            
            var statement = new RetriableBatchDmlStatement(batchCmd, updateCounts);
            
            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)statement).Retry(retryTransaction, 60);
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_Succeeds()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));
            
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();
            
            var batchCmd = connection.CreateBatchDmlCommand();
            batchCmd.Transaction = transaction;
            batchCmd.Add(sql1);
            batchCmd.Add(sql2);
            
            var caughtException = Assert.Throws<SpannerBatchNonQueryException>(() => batchCmd.ExecuteNonQuery());
            
            var statement = new FailedBatchDmlStatement(batchCmd, caughtException);
            
            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)statement).Retry(retryTransaction, 60);
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_SyncRetry_Succeeds()
        {
            string sql = "SELECT * FROM Foo WHERE Id = 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));
            
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();
            
            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;
            
            using var reader = command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);
            
            Assert.True(checksumReader.Read());
            Assert.Equal(1L, checksumReader.GetInt64(0));
            Assert.Equal("Test Name", checksumReader.GetString(1));
            Assert.False(checksumReader.Read());
            
            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60);
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_SyncRetry_FailsOnModifiedResults()
        {
            string sql = "SELECT * FROM Foo WHERE Id = 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));
            
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();
            
            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;
            
            using var reader = command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);
            
            Assert.True(checksumReader.Read());
            Assert.False(checksumReader.Read());
            
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Modified Name"]
                ]
            ));
            
            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60));
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_SyncRetry_AfterReaderDisposed_Succeeds()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            using var reader = command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            Assert.True(checksumReader.Read());
            Assert.Equal(1L, checksumReader.GetInt64(0));
            Assert.Equal("Test Name", checksumReader.GetString(1));
            Assert.False(checksumReader.Read());

            // Dispose the reader as EF Core or a user would do when finished reading rows.
            checksumReader.Dispose();
            Assert.True(checksumReader.IsClosed);

            // The underlying SpannerCommand must remain alive until transaction disposal, so retry succeeds.
            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60);
        }

        [Fact]
        public async Task SpannerDataReaderWithChecksum_AsyncRetry_AfterReaderDisposed_Succeeds()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, checksumReader.GetInt64(0));
            Assert.Equal("Test Name", checksumReader.GetString(1));
            Assert.False(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));

            // Dispose the reader.
            checksumReader.Dispose();
            Assert.True(checksumReader.IsClosed);

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)checksumReader).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_DisposalViaIDisposable_HasConsistentSemanticsAndAllowsRetry()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            using var reader = command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            Assert.True(checksumReader.Read());

            // Disposing via IDisposable reference must have identical semantics to DbDataReader.Dispose()
            // and must not prematurely dispose the underlying command.
            IDisposable disposableReader = checksumReader;
            disposableReader.Dispose();
            Assert.True(checksumReader.IsClosed);

            // The underlying SpannerCommand was not prematurely disposed, so retry succeeds.
            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60);
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_ActiveReaderRetry_DisposesOldReader()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name 1"],
                    [2L, "Test Name 2"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            var initialReader = (SpannerDataReader)command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, initialReader, command);

            // Read only 1 row so reader remains active.
            Assert.True(checksumReader.Read());
            Assert.Equal(1L, checksumReader.GetInt64(0));
            Assert.False(checksumReader.IsClosed);
            Assert.Same(initialReader, checksumReader.SpannerDataReader);

            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60);

            // The old reader should now be disposed/closed to prevent resource leaks.
            Assert.True(initialReader.IsClosed);
            Assert.NotSame(initialReader, checksumReader.SpannerDataReader);
            Assert.False(checksumReader.IsClosed);

            // Reading continues seamlessly on the newly acquired reader.
            Assert.True(checksumReader.Read());
            Assert.Equal(2L, checksumReader.GetInt64(0));
            Assert.False(checksumReader.Read());
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_MultipleConsecutiveRetries_Succeeds()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name 1"],
                    [2L, "Test Name 2"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            var firstReader = (SpannerDataReader)command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, firstReader, command);

            Assert.True(checksumReader.Read());

            // First retry
            using var retryTransaction1 = connection.BeginTransaction();
            ((IRetriableStatement)checksumReader).Retry(retryTransaction1, 60);
            Assert.True(firstReader.IsClosed);
            var secondReader = checksumReader.SpannerDataReader;
            Assert.NotSame(firstReader, secondReader);

            // Second retry
            using var retryTransaction2 = connection.BeginTransaction();
            ((IRetriableStatement)checksumReader).Retry(retryTransaction2, 60);
            Assert.True(secondReader.IsClosed);
            var thirdReader = checksumReader.SpannerDataReader;
            Assert.NotSame(secondReader, thirdReader);

            Assert.True(checksumReader.Read());
            Assert.Equal(2L, checksumReader.GetInt64(0));
            Assert.False(checksumReader.Read());

            checksumReader.Dispose();
            Assert.True(thirdReader.IsClosed);
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_SyncRetry_AfterReaderDisposed_FailsOnModifiedResults()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            using var reader = command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            Assert.True(checksumReader.Read());
            Assert.False(checksumReader.Read());

            // Dispose reader: final checksum is cached in _finalChecksum.
            checksumReader.Dispose();
            Assert.True(checksumReader.IsClosed);

            // Modify mock result for concurrent modification.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Modified Name"]
                ]
            ));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60));
        }

        [Fact]
        public async Task SpannerDataReaderWithChecksum_AsyncRetry_AfterReaderDisposed_FailsOnModifiedResults()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.False(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));

            // Dispose reader: final checksum is cached in _finalChecksum.
            checksumReader.Dispose();
            Assert.True(checksumReader.IsClosed);

            // Modify mock result for concurrent modification.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Modified Name"]
                ]
            ));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(async () =>
                await ((IRetriableStatement)checksumReader).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_SyncRetry_AfterReaderDisposed_FailsOnRowCountMismatch()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name 1"],
                    [2L, "Test Name 2"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            using var reader = command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            Assert.True(checksumReader.Read());
            Assert.True(checksumReader.Read());
            Assert.False(checksumReader.Read());

            checksumReader.Dispose();
            Assert.True(checksumReader.IsClosed);

            // Replay returns fewer rows (only 1 row instead of 2).
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name 1"]
                ]
            ));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60));
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_EmptyResultSet_AfterReaderDisposed_RetrySucceedsAndFailsOnModified()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                []
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            using var reader = command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            // 0 rows returned
            Assert.False(checksumReader.Read());

            checksumReader.Dispose();
            Assert.True(checksumReader.IsClosed);

            // Retry with same 0 rows succeeds.
            using var retryTransaction1 = connection.BeginTransaction();
            ((IRetriableStatement)checksumReader).Retry(retryTransaction1, 60);

            // Now mock returns 1 row (phantom row).
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Unexpected Row"]
                ]
            ));

            using var retryTransaction2 = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)checksumReader).Retry(retryTransaction2, 60));
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_StatementDispose_IsIdempotent()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            using var reader = command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            // First disposal
            checksumReader.Dispose();
            Assert.True(checksumReader.IsClosed);

            // Second disposal must be idempotent and not throw
            checksumReader.Dispose();
            Assert.True(checksumReader.IsClosed);
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_RetryPropagatesAbortedError()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            using var reader = command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            Assert.True(checksumReader.Read());
            Assert.False(checksumReader.Read());
            checksumReader.Dispose();

            // Set mock to return Aborted error on retry.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.Aborted, "Transaction was aborted"))));

            using var retryTransaction = connection.BeginTransaction();
            var exception = Assert.Throws<SpannerException>(() =>
                ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60));
            Assert.Equal(ErrorCode.Aborted, exception.ErrorCode);
        }

        [Fact]
        public async Task SpannerDataReaderWithChecksum_RetryPropagatesAbortedError_Async()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.False(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            await checksumReader.DisposeAsync();

            // Set mock to return Aborted error on retry.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.Aborted, "Transaction was aborted"))));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            var exception = await Assert.ThrowsAsync<SpannerException>(async () =>
                await ((IRetriableStatement)checksumReader).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
            Assert.Equal(ErrorCode.Aborted, exception.ErrorCode);
        }

        [Fact]
        public async Task SpannerDataReaderWithChecksum_ActiveReaderRetry_Async_DisposesOldReader()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name 1"],
                    [2L, "Test Name 2"]
                ]
            ));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            var initialReader = (SpannerDataReader)await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, initialReader, command);

            // Read only 1 row so reader remains active.
            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, checksumReader.GetInt64(0));
            Assert.False(checksumReader.IsClosed);
            Assert.Same(initialReader, checksumReader.SpannerDataReader);

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)checksumReader).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);

            // The old reader should now be disposed/closed to prevent resource leaks.
            Assert.True(initialReader.IsClosed);
            Assert.NotSame(initialReader, checksumReader.SpannerDataReader);
            Assert.False(checksumReader.IsClosed);

            // Reading continues seamlessly on the newly acquired reader.
            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(2L, checksumReader.GetInt64(0));
            Assert.False(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
        }

        [Fact]
        public async Task SpannerDataReaderWithChecksum_MultipleConsecutiveRetries_Async_Succeeds()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name 1"],
                    [2L, "Test Name 2"]
                ]
            ));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            var firstReader = (SpannerDataReader)await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, firstReader, command);

            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));

            // First retry
            using var retryTransaction1 = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)checksumReader).RetryAsync(retryTransaction1, TestContext.Current.CancellationToken, 60);
            Assert.True(firstReader.IsClosed);
            var secondReader = checksumReader.SpannerDataReader;
            Assert.NotSame(firstReader, secondReader);

            // Second retry
            using var retryTransaction2 = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)checksumReader).RetryAsync(retryTransaction2, TestContext.Current.CancellationToken, 60);
            Assert.True(secondReader.IsClosed);
            var thirdReader = checksumReader.SpannerDataReader;
            Assert.NotSame(secondReader, thirdReader);

            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(2L, checksumReader.GetInt64(0));
            Assert.False(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));

            await checksumReader.DisposeAsync();
            Assert.True(thirdReader.IsClosed);
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_ActiveReaderRetry_WithZeroReads_Succeeds()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            var initialReader = (SpannerDataReader)command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, initialReader, command);

            // 0 reads performed before retry occurs
            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60);

            Assert.True(initialReader.IsClosed);
            Assert.NotSame(initialReader, checksumReader.SpannerDataReader);
            Assert.False(checksumReader.IsClosed);

            // Reading functions normally on the new reader
            Assert.True(checksumReader.Read());
            Assert.Equal(1L, checksumReader.GetInt64(0));
            Assert.False(checksumReader.Read());
        }

        [Fact]
        public async Task SpannerDataReaderWithChecksum_DisposeAsync_SucceedsAndAllowsRetry()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            var spannerReader = (SpannerDataReader)await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, spannerReader, command);

            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.False(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));

            // Async disposal
            await checksumReader.DisposeAsync();
            Assert.True(checksumReader.IsClosed);
            Assert.True(spannerReader.IsClosed);

            // Replay after async disposal works seamlessly
            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)checksumReader).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);
        }

        [Fact]
        public async Task SpannerDataReaderWithChecksum_ActiveReaderRetry_WithZeroReads_Async_Succeeds()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            var initialReader = (SpannerDataReader)await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, initialReader, command);

            // 0 reads performed before async retry occurs
            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)checksumReader).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);

            Assert.True(initialReader.IsClosed);
            Assert.NotSame(initialReader, checksumReader.SpannerDataReader);
            Assert.False(checksumReader.IsClosed);

            // Reading functions normally on the new reader
            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, checksumReader.GetInt64(0));
            Assert.False(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
        }

        [Fact]
        public async Task SpannerDataReaderWithChecksum_AsyncRetry_AfterReaderDisposed_FailsOnRowCountMismatch()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name 1"],
                    [2L, "Test Name 2"]
                ]
            ));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.False(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));

            await checksumReader.DisposeAsync();
            Assert.True(checksumReader.IsClosed);

            // Replay returns fewer rows (only 1 row instead of 2).
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name 1"]
                ]
            ));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(async () =>
                await ((IRetriableStatement)checksumReader).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_ActiveReaderRetry_FailsOnRowCountMismatch()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name 1"],
                    [2L, "Test Name 2"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            var initialReader = (SpannerDataReader)command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, initialReader, command);

            // Active read: reads row 1 and row 2, reader is still open
            Assert.True(checksumReader.Read());
            Assert.True(checksumReader.Read());
            Assert.False(checksumReader.IsClosed);

            // Before finishing read, replay returns fewer rows
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name 1"]
                ]
            ));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60));
        }

        [Fact]
        public void SpannerDataReaderWithChecksum_SyncRetry_AfterReaderClosed_Succeeds()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            using var reader = command.ExecuteReader();
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, (SpannerDataReader)reader, command);

            Assert.True(checksumReader.Read());
            Assert.False(checksumReader.Read());

            // Close via Close() instead of Dispose()
            checksumReader.Close();
            Assert.True(checksumReader.IsClosed);

            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)checksumReader).Retry(retryTransaction, 60);
        }

        [Fact]
        public async Task SpannerDataReaderWithChecksum_MixedDisposal_IsIdempotent()
        {
            string sql = $"SELECT * FROM Foo WHERE Id = {_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                [
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                    Tuple.Create(V1.TypeCode.String, "Name"),
                ],
                [
                    [1L, "Test Name"]
                ]
            ));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var command = connection.SpannerConnection.CreateSelectCommand(sql);
            command.Transaction = transaction.SpannerTransaction;

            var spannerReader = (SpannerDataReader)await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            var checksumReader = new SpannerDataReaderWithChecksum(transaction, spannerReader, command);

            Assert.True(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.False(await checksumReader.ReadAsync(TestContext.Current.CancellationToken));

            // DisposeAsync followed by synchronous Dispose
            await checksumReader.DisposeAsync();
            Assert.True(checksumReader.IsClosed);

            checksumReader.Dispose();
            Assert.True(checksumReader.IsClosed);

            // Replay still succeeds
            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)checksumReader).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);
        }
    }
}
