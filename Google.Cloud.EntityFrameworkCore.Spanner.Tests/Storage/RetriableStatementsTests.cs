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
    }
}
