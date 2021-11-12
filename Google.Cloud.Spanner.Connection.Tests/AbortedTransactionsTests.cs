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

using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using System.Collections.Generic;
using Xunit;
using System.Threading.Tasks;
using System;
using System.Collections.Concurrent;

namespace Google.Cloud.Spanner.Connection.Tests
{
    public class AbortedTransactionsTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public AbortedTransactionsTests(SpannerMockServerFixture service)
        {
            _fixture = service;
            _fixture.SpannerMock.Reset();
        }

        private SpannerRetriableConnection CreateConnection()
        {
            var connectionString = $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";
            return new SpannerRetriableConnection(new SpannerConnection(connectionString, ChannelCredentials.Insecure));
        }

        [Fact]
        public async Task ReadWriteTransaction_WithoutAbort_DoesNotRetry()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();

            var cmd = connection.CreateDmlCommand(sql);
            cmd.Transaction = transaction;
            var updateCount = await cmd.ExecuteNonQueryAsync();
            await transaction.CommitAsync();
            Assert.Equal(1, updateCount);
            Assert.Equal(0, transaction.RetryCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_AbortedDml_IsAutomaticallyRetried(bool enableInternalRetries)
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            // Abort the transaction on the mock server. The transaction should be able to internally retry.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            var cmd = connection.CreateDmlCommand(sql);
            cmd.Transaction = transaction;
            if (enableInternalRetries)
            {
                var updateCount = await cmd.ExecuteNonQueryAsync();
                Assert.Equal(1, updateCount);
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var e = await Assert.ThrowsAsync<SpannerException>(() => cmd.ExecuteNonQueryAsync());
                Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            }
        }

        [Fact]
        public async Task ReadWriteTransaction_ModifiedDmlUpdateCount_FailsRetry()
        {
            // This statement returns an update count of 1 the first time.
            string sql = $"UPDATE Foo SET Bar='baz' WHERE Id IN ({_fixture.RandomLong()},{_fixture.RandomLong()})";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));

            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            // Execute an update and then change the return value for the statement before the retry is executed.
            var cmd = connection.CreateDmlCommand(sql);
            cmd.Transaction = transaction;
            Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
            // The update statement will return 2 the next time it is executed.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(2));

            // Now abort the transaction and try to execute another DML statement. The retry will fail because it sees
            // a different update count during the retry.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            cmd = connection.CreateDmlCommand($"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}");
            cmd.Transaction = transaction;
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => cmd.ExecuteNonQueryAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_AbortedDmlWithSameException_CanBeRetried(bool enableInternalRetries)
        {
            string sql = $"UPDATE Foo SET Bar='bar' Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.FailedPrecondition, "UPDATE statement misses WHERE clause"))));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateDmlCommand(sql);
            cmd.Transaction = transaction;

            var e = await Assert.ThrowsAsync<SpannerException>(() => cmd.ExecuteNonQueryAsync());
            Assert.Equal(ErrorCode.FailedPrecondition, e.ErrorCode);
            Assert.NotNull(e.InnerException);
            Assert.Contains("UPDATE statement misses WHERE clause", e.InnerException?.Message);

            // Abort the transaction on the mock server. The transaction should be able to internally retry.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            if (enableInternalRetries)
            {
                await transaction.CommitAsync();
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var se = await Assert.ThrowsAsync<SpannerException>(() => transaction.CommitAsync());
                Assert.Equal(ErrorCode.Aborted, se.ErrorCode);
            }
        }

        [Fact]
        public async Task ReadWriteTransaction_AbortedDmlWithDifferentException_FailsRetry()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.AlreadyExists, "Unique key constraint violation"))));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            var cmd = connection.CreateDmlCommand(sql);
            cmd.Transaction = transaction;
            var e = await Assert.ThrowsAsync<SpannerException>(() => cmd.ExecuteNonQueryAsync());
            Assert.Equal(ErrorCode.AlreadyExists, e.ErrorCode);
            Assert.NotNull(e.InnerException);
            Assert.Contains("Unique key constraint violation", e.InnerException.Message);

            // Change the error for the statement on the mock server and abort the transaction.
            // The retry should now fail as the error has changed.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Table Foo not found"))));
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => transaction.CommitAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_AbortedBatchDml_IsAutomaticallyRetried(bool enableInternalRetries)
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id IN ({_fixture.RandomLong()},{_fixture.RandomLong()})";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateBatchDmlCommand();
            cmd.Transaction = transaction;
            cmd.Add(sql1);
            cmd.Add(sql2);

            // Abort the transaction on the mock server. The transaction should be able to internally retry.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);

            if (enableInternalRetries)
            {
                var updateCounts = await cmd.ExecuteNonQueryAsync();
                Assert.Equal(new List<long> { 1, 2 }, updateCounts);
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var e = await Assert.ThrowsAsync<SpannerException>(() => cmd.ExecuteNonQueryAsync());
                Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            }
        }

        [Fact]
        public async Task ReadWriteTransaction_ModifiedBatchDmlUpdateCount_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='baz' WHERE Id IN ({_fixture.RandomLong()},{_fixture.RandomLong()})";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(2));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            var cmd = connection.CreateBatchDmlCommand();
            cmd.Transaction = transaction;
            cmd.Add(sql1);
            cmd.Add(sql2);
            Assert.Equal(new List<long> { 1, 2 }, await cmd.ExecuteNonQueryAsync());
            // Change the update count returned by one of the statements and abort the transaction.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);

            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => transaction.CommitAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_BatchDmlWithSameException_CanBeRetried(bool enableInternalRetries)
        {
            // UPDATE statement that misses a WHERE clause.
            string sql1 = $"UPDATE Foo SET Bar='bar' Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateException(new RpcException(new Status(StatusCode.FailedPrecondition, "UPDATE statement misses WHERE clause"))));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateBatchDmlCommand();
            cmd.Transaction = transaction;
            cmd.Add(sql1);

            var e = await Assert.ThrowsAsync<SpannerException>(() => cmd.ExecuteNonQueryAsync());
            Assert.Equal(ErrorCode.FailedPrecondition, e.ErrorCode);
            Assert.NotNull(e.InnerException);
            Assert.Contains("UPDATE statement misses WHERE clause", e.InnerException.Message);
            // Abort the transaction and try to commit. That will trigger a retry, and the retry will receive
            // the same error for the BatchDML call as the original attempt.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);

            if (enableInternalRetries)
            {
                await transaction.CommitAsync();
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var se = await Assert.ThrowsAsync<SpannerException>(() => transaction.CommitAsync());
                Assert.Equal(ErrorCode.Aborted, se.ErrorCode);
            }
        }

        [Fact]
        public async Task ReadWriteTransaction_BatchDmlWithDifferentException_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateException(new RpcException(new Status(StatusCode.AlreadyExists, "Unique key constraint violation"))));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            var cmd = connection.CreateBatchDmlCommand();
            cmd.Transaction = transaction;
            cmd.Add(sql1);
            try
            {
                await cmd.ExecuteNonQueryAsync();
                Assert.True(false, "Missing expected exception");
            }
            catch (SpannerException e) when (e.ErrorCode == ErrorCode.AlreadyExists)
            {
                Assert.NotNull(e.InnerException);
                Assert.Contains("Unique key constraint violation", e.InnerException.Message);
            }

            // Remove the error message for the update statement.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            // Abort the transaction and try to commit. That will trigger a retry, but the retry
            // will not receive an error for the update statement. That will fail the retry.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);

            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => transaction.CommitAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_BatchDmlWithSameExceptionHalfwayAndSameResults_CanBeRetried(bool enableInternalRetries)
        {
            string sql1 = $"UPDATE Foo SET Bar='valid' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='invalid' Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(new RpcException(new Status(StatusCode.FailedPrecondition, "UPDATE statement misses WHERE clause"))));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateBatchDmlCommand();
            cmd.Transaction = transaction;
            cmd.Add(sql1);
            cmd.Add(sql2);

            var e = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => cmd.ExecuteNonQueryAsync());
            Assert.Contains("UPDATE statement misses WHERE clause", e.Message);
            Assert.Equal(new List<long> { 1 }, e.SuccessfulCommandResults);

            // Abort the transaction and try to commit. That will trigger a retry, and the retry will receive
            // the same error and the same results for the BatchDML call as the original attempt.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            if (enableInternalRetries)
            {
                await transaction.CommitAsync();
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var se = await Assert.ThrowsAsync<SpannerException>(() => transaction.CommitAsync());
                Assert.Equal(ErrorCode.Aborted, se.ErrorCode);
            }
        }

        [Fact]
        public async Task ReadWriteTransaction_BatchDmlWithSameExceptionHalfwayAndDifferentResults_FailsRetry()
        {
            string sql1 = $"UPDATE Foo SET Bar='valid' WHERE Id={_fixture.RandomLong()}";
            string sql2 = $"UPDATE Foo SET Bar='invalid' Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(1));
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateException(new RpcException(new Status(StatusCode.FailedPrecondition, "UPDATE statement misses WHERE clause"))));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            var cmd = connection.CreateBatchDmlCommand();
            cmd.Transaction = transaction;
            cmd.Add(sql1);
            cmd.Add(sql2);
            var e = await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => cmd.ExecuteNonQueryAsync());
            Assert.Contains("UPDATE statement misses WHERE clause", e.Message);
            Assert.Equal(new List<long> { 1 }, e.SuccessfulCommandResults);

            // Change the result of the first statement and abort the transaction.
            // The retry should now fail, even though the error code and message is the same.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(2));
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => transaction.CommitAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_QueryFullyConsumed_CanBeRetried(bool enableInternalRetries)
        {
            string sql = $"SELECT Id FROM Foo WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    Assert.Equal(1, reader.GetInt64(reader.GetOrdinal("Id")));
                }
            }
            // Abort the transaction on the mock server. The transaction should be able to internally retry.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            if (enableInternalRetries)
            {
                await transaction.CommitAsync();
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var e = await Assert.ThrowsAsync<SpannerException>(() => transaction.CommitAsync());
                Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_QueryWithSameException_CanBeRetried(bool enableInternalRetries)
        {
            string sql = $"SELECT Id FROM Foo WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Table not found: Foo"))));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                // Any query error is thrown by the first call to reader.ReadAsync();
                var e = await Assert.ThrowsAsync<SpannerException>(() => reader.ReadAsync());
                Assert.Equal(ErrorCode.NotFound, e.ErrorCode);
                Assert.NotNull(e.InnerException);
                Assert.Contains("Table not found: Foo", e.InnerException.Message);
            }
            // Abort the transaction on the mock server. The transaction should be able to internally retry.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            if (enableInternalRetries)
            {
                await transaction.CommitAsync();
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var se = await Assert.ThrowsAsync<SpannerException>(() => transaction.CommitAsync());
                Assert.Equal(ErrorCode.Aborted, se.ErrorCode);
            }
        }

        [Fact]
        public async Task ReadWriteTransaction_QueryFullyConsumed_WithModifiedResults_FailsRetry()
        {
            string sql = $"SELECT Id FROM Foo WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    Assert.Equal(1, reader.GetInt64(reader.GetOrdinal("Id")));
                }
            }
            // Change the result of the query on the server and abort the transaction.
            // The retry should now fail.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 2));
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => transaction.CommitAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Fact]
        public async Task ReadWriteTransaction_QueryFullyConsumed_WithModifiedResultsAfterLastRow_FailsRetry()
        {
            var sql = $"SELECT Id FROM Foo WHERE Id IN ({_fixture.RandomLong()}, {_fixture.RandomLong()})";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    Assert.Equal(1, reader.GetInt64(reader.GetOrdinal("Id")));
                }
            }
            // Add a row to the result of the query on the server and abort the transaction. Even though the
            // original query did not see the additional row, it did see a 'false' being returned after consuming
            // the first row in the query, meaning that it knew that there were no more results.
            // The retry should now fail.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "Id"),
                },
                new List<object[]>
                {
                    new object[] { 1L },
                    new object[] { 2L },
                }
            ));
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => transaction.CommitAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Fact]
        public async Task ReadWriteTransaction_QueryWithError_AndThenDifferentError_FailsRetry()
        {
            string sql = $"SELECT Id FROM Foo WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Table not found: Foo"))));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                // Any query error is thrown by the first call to reader.ReadAsync();
                var e = await Assert.ThrowsAsync<SpannerException>(() => reader.ReadAsync());
                Assert.NotNull(e.InnerException);
                Assert.Contains("Table not found: Foo", e.InnerException.Message);
            }

            // Change the error returned by the query on the server and abort the transaction.
            // The retry should now fail.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.PermissionDenied, "Permission denied for table Foo"))));
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => transaction.CommitAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Fact]
        public async Task ReadWriteTransaction_QueryWithError_AndThenNoError_FailsRetry()
        {
            string sql = $"SELECT Id FROM Foo WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Table not found: Foo"))));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                // Any query error is thrown by the first call to reader.ReadAsync();
                var e = await Assert.ThrowsAsync<SpannerException>(() => reader.ReadAsync());
                Assert.NotNull(e.InnerException);
                Assert.Contains("Table not found: Foo", e.InnerException.Message);
            }
            // Remove the error returned by the query on the server and abort the transaction.
            // The retry should now fail.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1));
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => transaction.CommitAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Fact]
        public async Task ReadWriteTransaction_QueryFullyConsumed_AndThenError_FailsRetry()
        {
            var sql = $"SELECT Id FROM Foo WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    Assert.Equal(1, reader.GetInt64(reader.GetOrdinal("Id")));
                }
            }
            // Replace the result returned by the query on the server with an error and abort the transaction.
            // The retry should now fail.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Table not found: Foo"))));
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => transaction.CommitAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_QueryHalfConsumed_WithSameResults_CanBeRetried(bool enableInternalRetries)
        {
            string sql = $"SELECT Id FROM Foo WHERE Id IN ({_fixture.RandomLong()}, {_fixture.RandomLong()})";
            // Create a result set with 2 rows.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1, 2));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                // Only consume the first row of the reader.
                Assert.True(await reader.ReadAsync());
                Assert.Equal(1, reader.GetInt64(reader.GetOrdinal("Id")));
            }
            // Abort the transaction on the mock server. The transaction should be able to internally retry.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            if (enableInternalRetries)
            {
                await transaction.CommitAsync();
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var e = await Assert.ThrowsAsync<SpannerException>(() => transaction.CommitAsync());
                Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_QueryHalfConsumed_WithDifferentUnseenResults_CanBeRetried(bool enableInternalRetries)
        {
            string sql = $"SELECT Id FROM Foo WHERE Id IN ({_fixture.RandomLong()}, {_fixture.RandomLong()})";
            // Create a result set with 2 rows.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1, 2));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                // Only consume the first row of the reader.
                Assert.True(await reader.ReadAsync());
                Assert.Equal(1, reader.GetInt64(reader.GetOrdinal("Id")));
            }
            // Change the second row of the result of the query. That row has never been seen by the transaction
            // and should therefore not cause any retry to abort.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1, 3));
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            if (enableInternalRetries)
            {
                await transaction.CommitAsync();
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var e = await Assert.ThrowsAsync<SpannerException>(() => transaction.CommitAsync());
                Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_QueryAbortsHalfway_WithSameResults_CanBeRetried(bool enableInternalRetries)
        {
            string sql = $"SELECT Id FROM Foo WHERE Id IN ({_fixture.RandomLong()}, {_fixture.RandomLong()})";
            // Create a result set with 2 rows.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1, 2));
            // The following will cause the ExecuteStreamingSql method on the mock server to return an Aborted error on stream index 1 (i.e. before the row with value 2 is returned).
            // This simulates a transaction that is aborted while a streaming result set is still being returned to the client.
            var streamWritePermissions = new BlockingCollection<int>
            {
                1
            };
            var executionTime = ExecutionTime.StreamException(MockSpannerService.CreateAbortedException(sql), 1, streamWritePermissions);
            _fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteStreamingSql), executionTime);

            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using var reader = await cmd.ExecuteReaderAsync();
            // Only the first row of the reader.
            Assert.True(await reader.ReadAsync());
            Assert.Equal(1, reader.GetInt64(reader.GetOrdinal("Id")));

            // Try to get the second row of the result. This should succeed, even though the transaction
            // was aborted, retried and the reader was re-initialized under the hood.
            executionTime.AlwaysAllowWrite();
            if (enableInternalRetries)
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal(2, reader.GetInt64(reader.GetOrdinal("Id")));
                // Ensure that there are no more rows in the results.
                Assert.False(await reader.ReadAsync());
                // Check that the transaction really retried.
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var e = await Assert.ThrowsAsync<SpannerException>(() => reader.ReadAsync());
                Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_QueryAbortsHalfway_WithDifferentUnseenResults_CanBeRetried(bool enableInternalRetries)
        {
            string sql = $"SELECT Id FROM Foo WHERE Id IN ({_fixture.RandomLong()}, {_fixture.RandomLong()})";
            // Create a result set with 2 rows.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1, 2));
            // The following will cause the ExecuteStreamingSql method on the mock server to return an Aborted error on stream index 1 (i.e. before the row with value 2 is returned).
            // This simulates a transaction that is aborted while a streaming result set is still being returned to the client.
            var streamWritePermissions = new BlockingCollection<int>
            {
                1
            };
            var executionTime = ExecutionTime.StreamException(MockSpannerService.CreateAbortedException(sql), 1, streamWritePermissions);
            _fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteStreamingSql), executionTime);

            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using var reader = await cmd.ExecuteReaderAsync();
            // Only the first row of the reader.
            Assert.True(await reader.ReadAsync());
            Assert.Equal(1, reader.GetInt64(reader.GetOrdinal("Id")));

            // Now change the result of the query, but only for the second row which has not yet been
            // seen by this transaction.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1, 3));
            // Try to get the second row of the result. This should succeed, even though the transaction
            // was aborted, retried and the reader was re-initialized under the hood. The retry succeeds
            // because only data that had not yet been seen by this transaction was changed.
            executionTime.AlwaysAllowWrite();
            if (enableInternalRetries)
            {
                Assert.True(await reader.ReadAsync());
                Assert.Equal(3, reader.GetInt64(reader.GetOrdinal("Id")));
                // Ensure that there are no more rows in the results.
                Assert.False(await reader.ReadAsync());
                // Check that the transaction really retried.
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var e = await Assert.ThrowsAsync<SpannerException>(() => reader.ReadAsync());
                Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            }
        }

        [Fact]
        public async Task ReadWriteTransaction_QueryAbortsHalfway_WithDifferentResults_FailsRetry()
        {
            string sql = $"SELECT Id FROM Foo WHERE Id IN ({_fixture.RandomLong()}, {_fixture.RandomLong()})";
            // Create a result set with 2 rows.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1, 2));
            // The following will cause the ExecuteStreamingSql method on the mock server to return an Aborted error on stream index 1 (i.e. before the row with value 2 is returned).
            // This simulates a transaction that is aborted while a streaming result set is still being returned to the client.
            var streamWritePermissions = new BlockingCollection<int>
            {
                1
            };
            var executionTime = ExecutionTime.StreamException(MockSpannerService.CreateAbortedException(sql), 1, streamWritePermissions);
            _fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteStreamingSql), executionTime);

            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            using var reader = await cmd.ExecuteReaderAsync();
            // Only the first row of the reader.
            Assert.True(await reader.ReadAsync());
            Assert.Equal(1, reader.GetInt64(reader.GetOrdinal("Id")));

            // Now change the result of the query for the record that has already been seen.
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 3, 2));
            executionTime.AlwaysAllowWrite();
            // Try to get the second row of the result. This will now fail.
            await Assert.ThrowsAsync<SpannerAbortedDueToConcurrentModificationException>(() => reader.ReadAsync());
            Assert.Equal(1, transaction.RetryCount);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadWriteTransaction_ReadScalar_CanBeRetried(bool enableInternalRetries)
        {
            string sql = $"SELECT Id FROM Foo WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "Id", 1));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.EnableInternalRetries = enableInternalRetries;
            var cmd = connection.CreateSelectCommand(sql);
            cmd.Transaction = transaction;
            var id = await cmd.ExecuteScalarAsync();
            Assert.Equal(1L, id);
            // Abort the transaction on the mock server. The transaction should be able to internally retry.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            if (enableInternalRetries)
            {
                await transaction.CommitAsync();
                Assert.Equal(1, transaction.RetryCount);
            }
            else
            {
                var e = await Assert.ThrowsAsync<SpannerException>(() => transaction.CommitAsync());
                Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            }
        }

        [Fact]
        public async Task ReadWriteTransaction_Retry_GivesUp()
        {
            string sql = $"UPDATE Foo SET Bar='bar' WHERE Id={_fixture.RandomLong()}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
            using var connection = CreateConnection();
            await connection.OpenAsync();
            using var transaction = await connection.BeginTransactionAsync();
            transaction.MaxInternalRetryCount = 3;

            for (var i = 0; i < transaction.MaxInternalRetryCount; i++)
            {
                _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
                var cmd = connection.CreateDmlCommand(sql);
                cmd.Transaction = transaction;
                var updateCount = await cmd.ExecuteNonQueryAsync();
                Assert.Equal(1, updateCount);
                Assert.Equal(i + 1, transaction.RetryCount);
            }
            // The next statement that aborts will cause the transaction to fail.
            _fixture.SpannerMock.AbortTransaction(transaction.TransactionId);
            var cmd2 = connection.CreateDmlCommand(sql);
            cmd2.Transaction = transaction;
            var e = await Assert.ThrowsAsync<SpannerException>(() => cmd2.ExecuteNonQueryAsync());
            Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            Assert.Contains("Transaction was aborted because it aborted and retried too many times", e.Message);
        }
    }
}