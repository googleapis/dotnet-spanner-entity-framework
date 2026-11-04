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
using System.Linq;
using System.Reflection;
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
        public async Task RetriableBatchDmlStatement_AsyncRetry_Succeeds()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var updateCounts = await batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            Assert.Equal(new List<long> { 1, 2 }, updateCounts);

            var statement = new RetriableBatchDmlStatement(batchCommand, updateCounts);

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);
        }

        [Fact]
        public void RetriableBatchDmlStatement_SyncRetry_FailsOnModifiedCount()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var updateCounts = batchCommand.ExecuteNonQuery();
            Assert.Equal(new List<long> { 1, 2 }, updateCounts);

            var statement = new RetriableBatchDmlStatement(batchCommand, updateCounts);

            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(3));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public async Task RetriableBatchDmlStatement_AsyncRetry_FailsOnModifiedCount()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var updateCounts = await batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            Assert.Equal(new List<long> { 1, 2 }, updateCounts);

            var statement = new RetriableBatchDmlStatement(batchCommand, updateCounts);

            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(3));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }

        [Fact]
        public async Task RetriableBatchDmlStatement_MutationAfterExecution_DoesNotAffectRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            string sql3 = $"UPDATE Foo SET Bar='qux' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql3, StatementResult.CreateUpdateCount(3));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var updateCounts = await batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            Assert.Equal(new List<long> { 1, 2 }, updateCounts);

            var statement = new RetriableBatchDmlStatement(batchCommand, updateCounts);

            // Mutating batchCommand after recording it must not alter the cloned statement.
            batchCommand.Add(sql3);

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_Succeeds()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));

            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_FailsOnDifferentError()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = Assert.Throws<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQuery());
            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.Internal, "Internal error"))));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_FailsOnDifferentError()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.Internal, "Internal error"))));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }

        [Fact]
        public async Task FailedBatchDmlStatement_MutationAfterExecution_DoesNotAffectRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            string sql3 = $"UPDATE Foo SET Bar='qux' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql3, StatementResult.CreateUpdateCount(3));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            batchCommand.Add(sql3);

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);
        }

        [Fact]
        public void SpannerRetriableBatchCommand_ExecuteNonQuery_WithoutTransaction_Succeeds()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));

            using var connection = CreateConnection();
            connection.Open();

            using var batchCommand = connection.CreateBatchDmlCommand();
            Assert.Null(batchCommand.Transaction);
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var updateCounts = batchCommand.ExecuteNonQuery();
            Assert.Equal(new List<long> { 1, 2 }, updateCounts);
        }

        [Fact]
        public async Task SpannerRetriableBatchCommand_ExecuteNonQueryAsync_WithoutTransaction_Succeeds()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);

            using var batchCommand = connection.CreateBatchDmlCommand();
            Assert.Null(batchCommand.Transaction);
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var updateCounts = await batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            Assert.Equal(new List<long> { 1, 2 }, updateCounts);
        }

        [Fact]
        public void SpannerRetriableBatchCommand_Clone_CreatesIndependentDeepCopy()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            using var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);

            using var clone = batchCommand.Clone();
            Assert.Same(batchCommand.Connection, clone.Connection);
            Assert.Same(batchCommand.Transaction, clone.Transaction);

            clone.Add(sql2);

            var originalCounts = batchCommand.ExecuteNonQuery();
            Assert.Equal(new List<long> { 1 }, originalCounts);

            var cloneCounts = clone.ExecuteNonQuery();
            Assert.Equal(new List<long> { 1, 2 }, cloneCounts);
        }

        [Fact]
        public void SpannerRetriableBatchCommand_Dispose_DisposesCommands()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            using var connection = CreateConnection();
            var batchCommand = connection.CreateBatchDmlCommand();
            var command = connection.SpannerConnection.CreateDmlCommand(sql);
            batchCommand.Add(command);

            var commandsField = typeof(SpannerRetriableBatchCommand).GetField("_commands", BindingFlags.NonPublic | BindingFlags.Instance);
            var commands = (IList<SpannerCommand>)commandsField.GetValue(batchCommand);
            Assert.Single(commands);

            batchCommand.Dispose();

            Assert.Empty(commands);
            batchCommand.Dispose();
        }

        [Fact]
        public void SpannerRetriableBatchCommand_ICloneableClone_ReturnsDeepCopy()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            using var connection = CreateConnection();
            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Add(sql);

            var clone = Assert.IsType<SpannerRetriableBatchCommand>(((ICloneable)batchCommand).Clone());
            Assert.Same(batchCommand.Connection, clone.Connection);
        }

        [Fact]
        public void RetriableBatchDmlStatement_SyncRetry_FailsOnTimeout()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var updateCounts = batchCommand.ExecuteNonQuery();
            Assert.Equal(new List<long> { 1, 2 }, updateCounts);

            var statement = new RetriableBatchDmlStatement(batchCommand, updateCounts);

            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.DeadlineExceeded, "Deadline exceeded"))));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public async Task RetriableBatchDmlStatement_AsyncRetry_FailsOnTimeout()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var updateCounts = await batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            Assert.Equal(new List<long> { 1, 2 }, updateCounts);

            var statement = new RetriableBatchDmlStatement(batchCommand, updateCounts);

            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.DeadlineExceeded, "Deadline exceeded"))));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_Timeout_SucceedsWhenOriginalFailedWithTimeout()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.DeadlineExceeded, "Deadline exceeded"))));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = Assert.Throws<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQuery());
            Assert.Equal(ErrorCode.DeadlineExceeded, caughtException.ErrorCode);

            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)statement).Retry(retryTransaction, 60);
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_Timeout_SucceedsWhenOriginalFailedWithTimeout()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.DeadlineExceeded, "Deadline exceeded"))));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
            Assert.Equal(ErrorCode.DeadlineExceeded, caughtException.ErrorCode);

            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);
        }

        [Fact]
        public void SpannerRetriableBatchCommand_AddNullCommand_Throws()
        {
            using var connection = CreateConnection();
            using var batchCommand = connection.CreateBatchDmlCommand();
            Assert.Throws<ArgumentNullException>(() => batchCommand.Add((SpannerCommand)null));
        }

        [Fact]
        public void SpannerRetriableTransaction_ExecuteNonQueryWithRetry_SetsTransactionOnBatchCommand()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            using var batchCommand = connection.CreateBatchDmlCommand();
            Assert.Null(batchCommand.Transaction);

            batchCommand.Add(sql);
            var updateCounts = transaction.ExecuteNonQueryWithRetry(batchCommand);
            Assert.Equal(new List<long> { 1 }, updateCounts);
            Assert.Same(transaction, batchCommand.Transaction);
        }

        [Fact]
        public async Task SpannerRetriableTransaction_ExecuteNonQueryWithRetryAsync_SetsTransactionOnBatchCommand()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            using var batchCommand = connection.CreateBatchDmlCommand();
            Assert.Null(batchCommand.Transaction);

            batchCommand.Add(sql);
            var updateCounts = await transaction.ExecuteNonQueryWithRetryAsync(batchCommand, TestContext.Current.CancellationToken);
            Assert.Equal(new List<long> { 1 }, updateCounts);
            Assert.Same(transaction, batchCommand.Transaction);
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_GenericSpannerException_Succeeds()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid argument"))));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            using var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql);

            var caughtException = Assert.Throws<SpannerException>(() => batchCommand.ExecuteNonQuery());
            Assert.False(caughtException is SpannerBatchNonQueryException);
            Assert.Equal(ErrorCode.InvalidArgument, caughtException.ErrorCode);

            using var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)statement).Retry(retryTransaction, 60);
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_GenericSpannerException_Succeeds()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid argument"))));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            using var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql);

            var caughtException = await Assert.ThrowsAsync<SpannerException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
            Assert.False(caughtException is SpannerBatchNonQueryException);
            Assert.Equal(ErrorCode.InvalidArgument, caughtException.ErrorCode);

            using var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60);
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_UnexpectedSuccess_FailsRetry()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid argument"))));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            using var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql);

            var caughtException = Assert.Throws<SpannerException>(() => batchCommand.ExecuteNonQuery());

            using var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // Change statement result so that the retry attempt unexpectedly succeeds.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_UnexpectedSuccess_FailsRetry()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid argument"))));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            using var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql);

            var caughtException = await Assert.ThrowsAsync<SpannerException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));

            using var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // Change statement result so that the retry attempt unexpectedly succeeds.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }

        [Fact]
        public void RetriableBatchDmlStatement_Dispose_DisposesCommandClone()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            using var connection = CreateConnection();
            using var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Add(sql);

            var statement = new RetriableBatchDmlStatement(batchCommand, new List<long> { 1 });
            var commandField = typeof(RetriableBatchDmlStatement).GetField("_command", BindingFlags.NonPublic | BindingFlags.Instance);
            var commandClone = (SpannerRetriableBatchCommand)commandField.GetValue(statement);
            var commandsField = typeof(SpannerRetriableBatchCommand).GetField("_commands", BindingFlags.NonPublic | BindingFlags.Instance);
            var clonedCommands = (IList<SpannerCommand>)commandsField.GetValue(commandClone);
            Assert.Single(clonedCommands);

            statement.Dispose();

            Assert.Empty(clonedCommands);
            statement.Dispose();
        }

        [Fact]
        public void FailedBatchDmlStatement_Dispose_DisposesCommandClone()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            using var connection = CreateConnection();
            using var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Add(sql);

            var exception = new SpannerException(ErrorCode.InvalidArgument, "Invalid argument");
            var statement = new FailedBatchDmlStatement(batchCommand, exception);
            var commandField = typeof(FailedBatchDmlStatement).GetField("_command", BindingFlags.NonPublic | BindingFlags.Instance);
            var commandClone = (SpannerRetriableBatchCommand)commandField.GetValue(statement);
            var commandsField = typeof(SpannerRetriableBatchCommand).GetField("_commands", BindingFlags.NonPublic | BindingFlags.Instance);
            var clonedCommands = (IList<SpannerCommand>)commandsField.GetValue(commandClone);
            Assert.Single(clonedCommands);

            statement.Dispose();

            Assert.Empty(clonedCommands);
            statement.Dispose();
        }

        private static SpannerBatchNonQueryException CreateBatchException(ErrorCode errorCode, string message, IEnumerable<long> successfulCommandResults)
        {
            var constructors = typeof(SpannerBatchNonQueryException).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance);
            var constructor = constructors.First(c => c.GetParameters().Length == 2);
            var statusType = constructor.GetParameters()[0].ParameterType;
            var status = Activator.CreateInstance(statusType);
            statusType.GetProperty("Code")?.SetValue(status, (int)errorCode);
            statusType.GetProperty("Message")?.SetValue(status, message);
            return (SpannerBatchNonQueryException)constructor.Invoke(new object[] { status, successfulCommandResults });
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_ModifiedSuccessfulCommandResults_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = Assert.Throws<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQuery());
            Assert.Equal(new List<long> { 1 }, caughtException.SuccessfulCommandResults);

            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // Change the update count of statement 1 so that during retry, SuccessfulCommandResults has [2] instead of [1].
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(2));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_ModifiedSuccessfulCommandResults_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
            Assert.Equal(new List<long> { 1 }, caughtException.SuccessfulCommandResults);

            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // Change the update count of statement 1 so that during retry, SuccessfulCommandResults has [2] instead of [1].
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(2));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_DifferentCountOfSuccessfulCommandResults_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            string sql3 = $"UPDATE Foo SET Bar='qux' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql3, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);
            batchCommand.Add(sql3);

            var caughtException = Assert.Throws<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQuery());
            Assert.Equal(new List<long> { 1, 2 }, caughtException.SuccessfulCommandResults);

            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // Change statement 2 to also throw an error so retry has SuccessfulCommandResults = [1].
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_DifferentCountOfSuccessfulCommandResults_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            string sql3 = $"UPDATE Foo SET Bar='qux' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql3, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);
            batchCommand.Add(sql3);

            var caughtException = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
            Assert.Equal(new List<long> { 1, 2 }, caughtException.SuccessfulCommandResults);

            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // Change statement 2 to also throw an error so retry has SuccessfulCommandResults = [1].
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_InitialNullResults_RetryNonNullResults_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var initialException = CreateBatchException(ErrorCode.InvalidArgument, "Invalid statement", null);
            Assert.Null(initialException.SuccessfulCommandResults);

            var statement = new FailedBatchDmlStatement(batchCommand, initialException);

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_InitialNullResults_RetryNonNullResults_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var initialException = CreateBatchException(ErrorCode.InvalidArgument, "Invalid statement", null);
            Assert.Null(initialException.SuccessfulCommandResults);

            var statement = new FailedBatchDmlStatement(batchCommand, initialException);

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }

        [Fact]
        public void RetriableBatchDmlStatement_Sync_MutationAfterExecution_DoesNotAffectRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            string sql3 = $"UPDATE Foo SET Bar='qux' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql3, StatementResult.CreateUpdateCount(3));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var updateCounts = batchCommand.ExecuteNonQuery();
            Assert.Equal(new List<long> { 1, 2 }, updateCounts);

            var statement = new RetriableBatchDmlStatement(batchCommand, updateCounts);

            // Mutating batchCommand after recording it must not alter the cloned statement.
            batchCommand.Add(sql3);

            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)statement).Retry(retryTransaction, 60);
        }

        [Fact]
        public void FailedBatchDmlStatement_Sync_MutationAfterExecution_DoesNotAffectRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            string sql3 = $"UPDATE Foo SET Bar='qux' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql3, StatementResult.CreateUpdateCount(3));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = Assert.Throws<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQuery());
            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            batchCommand.Add(sql3);

            using var retryTransaction = connection.BeginTransaction();
            ((IRetriableStatement)statement).Retry(retryTransaction, 60);
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_BatchAbortedDuringRetry_BubblesUp()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = Assert.Throws<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQuery());
            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // Change statement 2 to return Aborted status during retry replay.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.Aborted, "Transaction was aborted"))));

            using var retryTransaction = connection.BeginTransaction();
            var e = Assert.Throws<SpannerBatchNonQueryException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
            Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_BatchAbortedDuringRetry_BubblesUp()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // Change statement 2 to return Aborted status during retry replay.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.Aborted, "Transaction was aborted"))));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            var e = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() =>
                ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
            Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_InitialBatchException_RetryGenericException_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = Assert.Throws<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQuery());
            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // During retry, make statement 1 fail so it throws a generic SpannerException (RPC level failure)
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_InitialBatchException_RetryGenericException_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // During retry, make statement 1 fail so it throws a generic SpannerException (RPC level failure)
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }

        [Fact]
        public void FailedBatchDmlStatement_SyncRetry_InitialGenericException_RetryBatchException_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));

            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = Assert.Throws<SpannerException>(() => batchCommand.ExecuteNonQuery());
            Assert.False(caughtException is SpannerBatchNonQueryException);
            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // During retry, statement 1 succeeds but statement 2 fails, throwing SpannerBatchNonQueryException
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var retryTransaction = connection.BeginTransaction();
            Assert.Throws<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).Retry(retryTransaction, 60));
        }

        [Fact]
        public async Task FailedBatchDmlStatement_AsyncRetry_InitialGenericException_RetryBatchException_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));

            using var connection = CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var transaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);

            var batchCommand = connection.CreateBatchDmlCommand();
            batchCommand.Transaction = transaction;
            batchCommand.Add(sql1);
            batchCommand.Add(sql2);

            var caughtException = await Assert.ThrowsAsync<SpannerException>(() => batchCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
            Assert.False(caughtException is SpannerBatchNonQueryException);
            var statement = new FailedBatchDmlStatement(batchCommand, caughtException);

            // During retry, statement 1 succeeds but statement 2 fails, throwing SpannerBatchNonQueryException
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

            using var retryTransaction = await connection.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() =>
                ((IRetriableStatement)statement).RetryAsync(retryTransaction, TestContext.Current.CancellationToken, 60));
        }
    }
}
