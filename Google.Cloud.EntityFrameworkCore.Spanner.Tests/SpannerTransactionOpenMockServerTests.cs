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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TransactionScope = System.Transactions.TransactionScope;
using SpannerType = Google.Cloud.Spanner.V1.Type;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;
using Xunit;

#pragma warning disable EF1001
namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
    public class SpannerTransactionOpenMockServerTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public SpannerTransactionOpenMockServerTests(SpannerMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        private class TestTransactionDbContext : DbContext
        {
            private readonly string _connectionString;
            private readonly IInterceptor _interceptor;

            public TestTransactionDbContext(string connectionString, IInterceptor interceptor = null)
            {
                _connectionString = connectionString;
                _interceptor = interceptor;
            }

            protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            {
                if (!optionsBuilder.IsConfigured)
                {
                    optionsBuilder
                        .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                        .UseMutations(MutationUsage.Never);

                    if (_interceptor != null)
                    {
                        optionsBuilder.AddInterceptors(_interceptor);
                    }
                }
            }
        }

        private class RecordingTransactionInterceptor : DbTransactionInterceptor
        {
            public int StartingCount { get; private set; }
            public int StartedCount { get; private set; }
            public int StartingAsyncCount { get; private set; }
            public int StartedAsyncCount { get; private set; }
            public IsolationLevel LastStartingIsolationLevel { get; private set; }
            public IsolationLevel LastStartingAsyncIsolationLevel { get; private set; }

            public override InterceptionResult<DbTransaction> TransactionStarting(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result)
            {
                StartingCount++;
                LastStartingIsolationLevel = eventData.IsolationLevel;
                return base.TransactionStarting(connection, eventData, result);
            }

            public override DbTransaction TransactionStarted(
                DbConnection connection,
                TransactionEndEventData eventData,
                DbTransaction result)
            {
                StartedCount++;
                return base.TransactionStarted(connection, eventData, result);
            }

            public override ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result,
                CancellationToken cancellationToken = default)
            {
                StartingAsyncCount++;
                LastStartingAsyncIsolationLevel = eventData.IsolationLevel;
                return base.TransactionStartingAsync(connection, eventData, result, cancellationToken);
            }

            public override ValueTask<DbTransaction> TransactionStartedAsync(
                DbConnection connection,
                TransactionEndEventData eventData,
                DbTransaction result,
                CancellationToken cancellationToken = default)
            {
                StartedAsyncCount++;
                return base.TransactionStartedAsync(connection, eventData, result, cancellationToken);
            }
        }

        private class DummyDbTransaction : DbTransaction
        {
            private readonly DbConnection _connection;
            private readonly bool _throwOnDispose;

            public DummyDbTransaction(DbConnection connection, bool throwOnDispose = false)
            {
                _connection = connection;
                _throwOnDispose = throwOnDispose;
            }

            public override IsolationLevel IsolationLevel => IsolationLevel.Serializable;
            protected override DbConnection DbConnection => _connection;
            public override void Commit() { }
            public override void Rollback() { }
            public bool IsDisposed { get; private set; }

            protected override void Dispose(bool disposing)
            {
                IsDisposed = true;
                if (_throwOnDispose)
                {
                    throw new InvalidOperationException("Dispose failed");
                }
                base.Dispose(disposing);
            }

            public override ValueTask DisposeAsync()
            {
                IsDisposed = true;
                if (_throwOnDispose)
                {
                    throw new InvalidOperationException("DisposeAsync failed");
                }
                return base.DisposeAsync();
            }
        }

        private class SuppressingTransactionInterceptor : DbTransactionInterceptor
        {
            public DummyDbTransaction SuppressedTransaction { get; private set; }

            public override InterceptionResult<DbTransaction> TransactionStarting(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result)
            {
                SuppressedTransaction = new DummyDbTransaction(connection);
                return InterceptionResult<DbTransaction>.SuppressWithResult(SuppressedTransaction);
            }

            public override ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result,
                CancellationToken cancellationToken = default)
            {
                SuppressedTransaction = new DummyDbTransaction(connection);
                return new ValueTask<InterceptionResult<DbTransaction>>(
                    InterceptionResult<DbTransaction>.SuppressWithResult(SuppressedTransaction));
            }
        }

        private class NullSuppressingTransactionInterceptor : DbTransactionInterceptor
        {
            public override InterceptionResult<DbTransaction> TransactionStarting(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result)
                => InterceptionResult<DbTransaction>.SuppressWithResult(null);

            public override ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result,
                CancellationToken cancellationToken = default)
                => new ValueTask<InterceptionResult<DbTransaction>>(
                    InterceptionResult<DbTransaction>.SuppressWithResult(null));
        }

        private class FailingStartingTransactionInterceptor : DbTransactionInterceptor
        {
            public override InterceptionResult<DbTransaction> TransactionStarting(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result)
                => throw new InvalidOperationException("TransactionStarting failed");

            public override ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result,
                CancellationToken cancellationToken = default)
                => throw new InvalidOperationException("TransactionStartingAsync failed");
        }

        private class CancellingStartingTransactionInterceptor : DbTransactionInterceptor
        {
            private readonly CancellationTokenSource _cancellationTokenSource;

            public CancellingStartingTransactionInterceptor(CancellationTokenSource cancellationTokenSource)
            {
                _cancellationTokenSource = cancellationTokenSource;
            }

            public override ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result,
                CancellationToken cancellationToken = default)
            {
                _cancellationTokenSource.Cancel();
                return base.TransactionStartingAsync(connection, eventData, result, cancellationToken);
            }
        }

        private class CancellingAndSuppressingStartingTransactionInterceptor : DbTransactionInterceptor
        {
            private readonly CancellationTokenSource _cancellationTokenSource;
            public DummyDbTransaction SuppressedTransaction { get; private set; }

            public CancellingAndSuppressingStartingTransactionInterceptor(CancellationTokenSource cancellationTokenSource)
            {
                _cancellationTokenSource = cancellationTokenSource;
            }

            public override ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result,
                CancellationToken cancellationToken = default)
            {
                _cancellationTokenSource.Cancel();
                SuppressedTransaction = new DummyDbTransaction(connection);
                return new ValueTask<InterceptionResult<DbTransaction>>(
                    InterceptionResult<DbTransaction>.SuppressWithResult(SuppressedTransaction));
            }
        }

        private class SuppressingAndFailingStartedTransactionInterceptor : DbTransactionInterceptor
        {
            private readonly bool _throwOnDispose;

            public SuppressingAndFailingStartedTransactionInterceptor(bool throwOnDispose = false)
            {
                _throwOnDispose = throwOnDispose;
            }

            public DummyDbTransaction SuppressedTransaction { get; private set; }

            public override InterceptionResult<DbTransaction> TransactionStarting(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result)
            {
                SuppressedTransaction = new DummyDbTransaction(connection, _throwOnDispose);
                return InterceptionResult<DbTransaction>.SuppressWithResult(SuppressedTransaction);
            }

            public override ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(
                DbConnection connection,
                TransactionStartingEventData eventData,
                InterceptionResult<DbTransaction> result,
                CancellationToken cancellationToken = default)
            {
                SuppressedTransaction = new DummyDbTransaction(connection, _throwOnDispose);
                return new ValueTask<InterceptionResult<DbTransaction>>(
                    InterceptionResult<DbTransaction>.SuppressWithResult(SuppressedTransaction));
            }

            public override DbTransaction TransactionStarted(
                DbConnection connection,
                TransactionEndEventData eventData,
                DbTransaction result)
                => throw new InvalidOperationException("TransactionStarted failed");

            public override ValueTask<DbTransaction> TransactionStartedAsync(
                DbConnection connection,
                TransactionEndEventData eventData,
                DbTransaction result,
                CancellationToken cancellationToken = default)
                => throw new InvalidOperationException("TransactionStartedAsync failed");
        }

        private class DummyTransactionManager : IDbContextTransactionManager
        {
            public IDbContextTransaction CurrentTransaction => null;
            public IDbContextTransaction BeginTransaction() => throw new NotImplementedException();
            public Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public void CommitTransaction() { }
            public Task CommitTransactionAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
            public void RollbackTransaction() { }
            public Task RollbackTransactionAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
            public void ResetState() { }
            public Task ResetStateAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        }

        private class NonSpannerDbContext : DbContext
        {
            private readonly string _connectionString;

            public NonSpannerDbContext(string connectionString)
            {
                _connectionString = connectionString;
            }

            protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            {
                if (!optionsBuilder.IsConfigured)
                {
                    optionsBuilder
                        .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                        .UseMutations(MutationUsage.Never)
                        .ReplaceService<IDbContextTransactionManager, DummyTransactionManager>();
                }
            }
        }

        [Fact]
        public void BeginTransaction_WithTag_OpensAndClosesConnectionWhenOwned()
        {
            _fixture.SpannerMock.Reset();
            _fixture.SpannerMock.AddOrUpdateStatementResult(
                "SELECT 1",
                StatementResult.CreateSingleColumnResultSet(1L, new SpannerType { Code = TypeCode.Int64 }, "col", 1L));
            using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            using (var transaction = context.Database.BeginTransaction("test-tag"))
            {
                Assert.NotNull(transaction);
                Assert.Equal(ConnectionState.Open, connection.State);
                Assert.Same(transaction, context.Database.CurrentTransaction);

                using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.Transaction = transaction.GetDbTransaction();
                using var reader = command.ExecuteReader();
                Assert.True(reader.Read());

                transaction.Commit();
            }

            Assert.Null(context.Database.CurrentTransaction);
            Assert.Equal(ConnectionState.Closed, connection.State);

            var sqlRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal("test-tag", sqlRequest.RequestOptions?.TransactionTag);
            Assert.Equal(TransactionOptions.ModeOneofCase.ReadWrite, sqlRequest.Transaction?.Begin?.ModeCase);

            var commitRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<CommitRequest>());
            Assert.Equal("test-tag", commitRequest.RequestOptions?.TransactionTag);
        }

        [Fact]
        public void BeginTransaction_WithIsolationLevelAndTag_OpensAndClosesConnectionWhenOwned()
        {
            _fixture.SpannerMock.Reset();
            _fixture.SpannerMock.AddOrUpdateStatementResult(
                "SELECT 1",
                StatementResult.CreateSingleColumnResultSet(1L, new SpannerType { Code = TypeCode.Int64 }, "col", 1L));
            using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            using (var transaction = context.Database.BeginTransaction(IsolationLevel.Serializable, "test-tag"))
            {
                Assert.NotNull(transaction);
                Assert.Equal(ConnectionState.Open, connection.State);
                Assert.Same(transaction, context.Database.CurrentTransaction);

                using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.Transaction = transaction.GetDbTransaction();
                using var reader = command.ExecuteReader();
                Assert.True(reader.Read());

                transaction.Commit();
            }

            Assert.Null(context.Database.CurrentTransaction);
            Assert.Equal(ConnectionState.Closed, connection.State);

            var sqlRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal("test-tag", sqlRequest.RequestOptions?.TransactionTag);
            Assert.Equal(TransactionOptions.ModeOneofCase.ReadWrite, sqlRequest.Transaction?.Begin?.ModeCase);

            var commitRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<CommitRequest>());
            Assert.Equal("test-tag", commitRequest.RequestOptions?.TransactionTag);
        }

        [Fact]
        public async Task BeginTransactionAsync_WithTag_OpensAndClosesConnectionWhenOwned()
        {
            _fixture.SpannerMock.Reset();
            _fixture.SpannerMock.AddOrUpdateStatementResult(
                "SELECT 1",
                StatementResult.CreateSingleColumnResultSet(1L, new SpannerType { Code = TypeCode.Int64 }, "col", 1L));
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            var transaction = await context.Database.BeginTransactionAsync("test-tag", TestContext.Current.CancellationToken);
            Assert.NotNull(transaction);
            Assert.Equal(ConnectionState.Open, connection.State);
            Assert.Same(transaction, context.Database.CurrentTransaction);

            await using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT 1";
                command.Transaction = transaction.GetDbTransaction();
                await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
                Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
            }

            await transaction.CommitAsync(TestContext.Current.CancellationToken);
            Assert.Null(context.Database.CurrentTransaction);
            Assert.Equal(ConnectionState.Closed, connection.State);

            var sqlRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal("test-tag", sqlRequest.RequestOptions?.TransactionTag);
            Assert.Equal(TransactionOptions.ModeOneofCase.ReadWrite, sqlRequest.Transaction?.Begin?.ModeCase);

            var commitRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<CommitRequest>());
            Assert.Equal("test-tag", commitRequest.RequestOptions?.TransactionTag);
        }

        [Fact]
        public async Task BeginTransactionAsync_WithIsolationLevelAndTag_OpensAndClosesConnectionWhenOwned()
        {
            _fixture.SpannerMock.Reset();
            _fixture.SpannerMock.AddOrUpdateStatementResult(
                "SELECT 1",
                StatementResult.CreateSingleColumnResultSet(1L, new SpannerType { Code = TypeCode.Int64 }, "col", 1L));
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            var transaction = await context.Database.BeginTransactionAsync(IsolationLevel.Serializable, "test-tag", TestContext.Current.CancellationToken);
            Assert.NotNull(transaction);
            Assert.Equal(ConnectionState.Open, connection.State);
            Assert.Same(transaction, context.Database.CurrentTransaction);

            await using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT 1";
                command.Transaction = transaction.GetDbTransaction();
                await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
                Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
            }

            await transaction.CommitAsync(TestContext.Current.CancellationToken);
            Assert.Null(context.Database.CurrentTransaction);
            Assert.Equal(ConnectionState.Closed, connection.State);

            var sqlRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal("test-tag", sqlRequest.RequestOptions?.TransactionTag);
            Assert.Equal(TransactionOptions.ModeOneofCase.ReadWrite, sqlRequest.Transaction?.Begin?.ModeCase);

            var commitRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<CommitRequest>());
            Assert.Equal("test-tag", commitRequest.RequestOptions?.TransactionTag);
        }

        [Fact]
        public void BeginReadOnlyTransaction_OpensAndClosesConnectionWhenOwned()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            using (var transaction = context.Database.BeginReadOnlyTransaction())
            {
                Assert.NotNull(transaction);
                Assert.Equal(ConnectionState.Open, connection.State);
                Assert.Same(transaction, context.Database.CurrentTransaction);
            }

            Assert.Null(context.Database.CurrentTransaction);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public void BeginReadOnlyTransaction_WithTimestampBound_OpensAndClosesConnectionWhenOwned()
        {
            _fixture.SpannerMock.Reset();
            using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            _fixture.SpannerMock.AddOrUpdateStatementResult(
                "SELECT 1",
                StatementResult.CreateSingleColumnResultSet(1L, new SpannerType { Code = TypeCode.Int64 }, "col", 1L));
            using (var transaction = context.Database.BeginReadOnlyTransaction(TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(15))))
            {
                Assert.NotNull(transaction);
                Assert.Equal(ConnectionState.Open, connection.State);
                Assert.Same(transaction, context.Database.CurrentTransaction);

                using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1";
                command.Transaction = transaction.GetDbTransaction();
                using var reader = command.ExecuteReader();
                Assert.True(reader.Read());
            }

            Assert.Null(context.Database.CurrentTransaction);
            Assert.Equal(ConnectionState.Closed, connection.State);

            var sqlRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.NotNull(sqlRequest.Transaction?.Begin?.ReadOnly?.ExactStaleness);
            Assert.Equal(15, sqlRequest.Transaction.Begin.ReadOnly.ExactStaleness.Seconds);
        }

        [Fact]
        public async Task BeginReadOnlyTransactionAsync_OpensAndClosesConnectionWhenOwned()
        {
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            var transaction = await context.Database.BeginReadOnlyTransactionAsync(TestContext.Current.CancellationToken);
            Assert.NotNull(transaction);
            Assert.Equal(ConnectionState.Open, connection.State);
            Assert.Same(transaction, context.Database.CurrentTransaction);

            await transaction.DisposeAsync();
            Assert.Null(context.Database.CurrentTransaction);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginReadOnlyTransactionAsync_WithTimestampBound_OpensAndClosesConnectionWhenOwned()
        {
            _fixture.SpannerMock.Reset();
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            _fixture.SpannerMock.AddOrUpdateStatementResult(
                "SELECT 1",
                StatementResult.CreateSingleColumnResultSet(1L, new SpannerType { Code = TypeCode.Int64 }, "col", 1L));
            var transaction = await context.Database.BeginReadOnlyTransactionAsync(
                TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(15)), TestContext.Current.CancellationToken);
            Assert.NotNull(transaction);
            Assert.Equal(ConnectionState.Open, connection.State);
            Assert.Same(transaction, context.Database.CurrentTransaction);

            await using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT 1";
                command.Transaction = transaction.GetDbTransaction();
                await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
                Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
            }

            await transaction.DisposeAsync();
            Assert.Null(context.Database.CurrentTransaction);
            Assert.Equal(ConnectionState.Closed, connection.State);

            var sqlRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.NotNull(sqlRequest.Transaction?.Begin?.ReadOnly?.ExactStaleness);
            Assert.Equal(15, sqlRequest.Transaction.Begin.ReadOnly.ExactStaleness.Seconds);
        }

        [Fact]
        public void BeginTransaction_WhenConnectionOpenedManually_PreservesOpenConnectionOnDispose()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            context.Database.OpenConnection();
            Assert.Equal(ConnectionState.Open, connection.State);

            using (var transaction = context.Database.BeginTransaction("test-tag"))
            {
                Assert.NotNull(transaction);
                Assert.Equal(ConnectionState.Open, connection.State);
            }

            Assert.Equal(ConnectionState.Open, connection.State);

            context.Database.CloseConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenConnectionOpenedManuallyAsync_PreservesOpenConnectionOnDispose()
        {
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            await context.Database.OpenConnectionAsync(TestContext.Current.CancellationToken);
            Assert.Equal(ConnectionState.Open, connection.State);

            var transaction = await context.Database.BeginTransactionAsync("test-tag", TestContext.Current.CancellationToken);
            Assert.NotNull(transaction);
            Assert.Equal(ConnectionState.Open, connection.State);

            await transaction.DisposeAsync();
            Assert.Equal(ConnectionState.Open, connection.State);

            await context.Database.CloseConnectionAsync();
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public void BeginReadOnlyTransaction_WhenConnectionOpenedManually_PreservesOpenConnectionOnDispose()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            context.Database.OpenConnection();
            Assert.Equal(ConnectionState.Open, connection.State);

            using (var transaction = context.Database.BeginReadOnlyTransaction(TimestampBound.Strong))
            {
                Assert.NotNull(transaction);
                Assert.Equal(ConnectionState.Open, connection.State);
            }

            Assert.Equal(ConnectionState.Open, connection.State);

            context.Database.CloseConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginReadOnlyTransactionAsync_WhenConnectionOpenedManuallyAsync_PreservesOpenConnectionOnDispose()
        {
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);

            await context.Database.OpenConnectionAsync(TestContext.Current.CancellationToken);
            Assert.Equal(ConnectionState.Open, connection.State);

            var transaction = await context.Database.BeginReadOnlyTransactionAsync(
                TimestampBound.Strong, TestContext.Current.CancellationToken);
            Assert.NotNull(transaction);
            Assert.Equal(ConnectionState.Open, connection.State);

            await transaction.DisposeAsync();
            Assert.Equal(ConnectionState.Open, connection.State);

            await context.Database.CloseConnectionAsync();
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public void TransactionInterceptors_AreInvoked_OnSynchronousBeginTransaction()
        {
            var interceptor = new RecordingTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            using (var transaction = context.Database.BeginTransaction(IsolationLevel.Serializable, "custom-tag"))
            {
                Assert.NotNull(transaction);
                Assert.Equal(1, interceptor.StartingCount);
                Assert.Equal(1, interceptor.StartedCount);
                Assert.Equal(IsolationLevel.Serializable, interceptor.LastStartingIsolationLevel);
            }
        }

        [Fact]
        public async Task TransactionInterceptors_AreInvoked_OnAsynchronousBeginTransaction()
        {
            var interceptor = new RecordingTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            var transaction = await context.Database.BeginTransactionAsync(
                IsolationLevel.Serializable, "async-tag", TestContext.Current.CancellationToken);
            Assert.NotNull(transaction);
            Assert.Equal(1, interceptor.StartingAsyncCount);
            Assert.Equal(1, interceptor.StartedAsyncCount);
            Assert.Equal(IsolationLevel.Serializable, interceptor.LastStartingAsyncIsolationLevel);

            await transaction.DisposeAsync();
        }

        [Fact]
        public void TransactionInterceptors_AreInvoked_OnSynchronousBeginTransaction_WithTagOnly()
        {
            var interceptor = new RecordingTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            using (var transaction = context.Database.BeginTransaction("custom-tag"))
            {
                Assert.NotNull(transaction);
                Assert.Equal(1, interceptor.StartingCount);
                Assert.Equal(1, interceptor.StartedCount);
                Assert.Equal(IsolationLevel.Unspecified, interceptor.LastStartingIsolationLevel);
            }
        }

        [Fact]
        public async Task TransactionInterceptors_AreInvoked_OnAsynchronousBeginTransaction_WithTagOnly()
        {
            var interceptor = new RecordingTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            var transaction = await context.Database.BeginTransactionAsync(
                "async-tag", TestContext.Current.CancellationToken);
            Assert.NotNull(transaction);
            Assert.Equal(1, interceptor.StartingAsyncCount);
            Assert.Equal(1, interceptor.StartedAsyncCount);
            Assert.Equal(IsolationLevel.Unspecified, interceptor.LastStartingAsyncIsolationLevel);

            await transaction.DisposeAsync();
        }

        [Fact]
        public void TransactionInterceptors_AreInvoked_OnSynchronousBeginReadOnlyTransaction()
        {
            var interceptor = new RecordingTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            using (var transaction = context.Database.BeginReadOnlyTransaction(TimestampBound.Strong))
            {
                Assert.NotNull(transaction);
                Assert.Equal(1, interceptor.StartingCount);
                Assert.Equal(1, interceptor.StartedCount);
                Assert.Equal(IsolationLevel.Unspecified, interceptor.LastStartingIsolationLevel);
            }
        }

        [Fact]
        public async Task TransactionInterceptors_AreInvoked_OnAsynchronousBeginReadOnlyTransaction()
        {
            var interceptor = new RecordingTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            var transaction = await context.Database.BeginReadOnlyTransactionAsync(
                TimestampBound.Strong, TestContext.Current.CancellationToken);
            Assert.NotNull(transaction);
            Assert.Equal(1, interceptor.StartingAsyncCount);
            Assert.Equal(1, interceptor.StartedAsyncCount);
            Assert.Equal(IsolationLevel.Unspecified, interceptor.LastStartingAsyncIsolationLevel);

            await transaction.DisposeAsync();
        }

        [Fact]
        public void BeginTransaction_WhenConnectionAlreadyHasTransaction_ThrowsInvalidOperationException()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            using var transaction = context.Database.BeginTransaction("first");

            Assert.Throws<InvalidOperationException>(() => context.Database.BeginTransaction("second"));
            Assert.Throws<InvalidOperationException>(() => context.Database.BeginTransaction(IsolationLevel.Serializable, "second"));
            Assert.Throws<InvalidOperationException>(() => context.Database.BeginReadOnlyTransaction());
            Assert.Throws<InvalidOperationException>(() => context.Database.BeginReadOnlyTransaction(TimestampBound.Strong));
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenConnectionAlreadyHasTransaction_ThrowsInvalidOperationException()
        {
            await using var context = new TestTransactionDbContext(ConnectionString);
            var transaction = await context.Database.BeginTransactionAsync("first", TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginTransactionAsync("second", TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginTransactionAsync(IsolationLevel.Serializable, "second", TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginReadOnlyTransactionAsync(TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginReadOnlyTransactionAsync(TimestampBound.Strong, TestContext.Current.CancellationToken));

            await transaction.DisposeAsync();
        }

        [Fact]
        public void BeginTransaction_WhenInterceptorSuppresses_UsesSuppressedTransaction()
        {
            var interceptor = new SuppressingTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            using (var transaction = context.Database.BeginTransaction("suppressed-tag"))
            {
                Assert.NotNull(interceptor.SuppressedTransaction);
                Assert.False(interceptor.SuppressedTransaction.IsDisposed);
                Assert.Equal(ConnectionState.Open, context.Database.GetDbConnection().State);
            }

            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenInterceptorSuppresses_UsesSuppressedTransaction()
        {
            var interceptor = new SuppressingTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            var transaction = await context.Database.BeginTransactionAsync("suppressed-tag", TestContext.Current.CancellationToken);
            Assert.NotNull(interceptor.SuppressedTransaction);
            Assert.False(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Open, context.Database.GetDbConnection().State);

            await transaction.DisposeAsync();
            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
        }

        [Fact]
        public void BeginReadOnlyTransaction_WhenInterceptorSuppresses_UsesSuppressedTransaction()
        {
            var interceptor = new SuppressingTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            using (var transaction = context.Database.BeginReadOnlyTransaction())
            {
                Assert.NotNull(interceptor.SuppressedTransaction);
                Assert.False(interceptor.SuppressedTransaction.IsDisposed);
                Assert.Equal(ConnectionState.Open, context.Database.GetDbConnection().State);
            }

            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
        }

        [Fact]
        public async Task BeginReadOnlyTransactionAsync_WhenInterceptorSuppresses_UsesSuppressedTransaction()
        {
            var interceptor = new SuppressingTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            var transaction = await context.Database.BeginReadOnlyTransactionAsync(TestContext.Current.CancellationToken);
            Assert.NotNull(interceptor.SuppressedTransaction);
            Assert.False(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Open, context.Database.GetDbConnection().State);

            await transaction.DisposeAsync();
            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
        }

        [Fact]
        public void BeginTransaction_WhenInterceptorSuppressesWithNull_DoesNotCallBeginTransaction()
        {
            var interceptor = new NullSuppressingTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            Assert.Throws<NullReferenceException>(() => context.Database.BeginTransaction("suppressed-tag"));
            Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenInterceptorSuppressesWithNull_DoesNotCallBeginTransaction()
        {
            var interceptor = new NullSuppressingTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            await Assert.ThrowsAsync<NullReferenceException>(
                () => context.Database.BeginTransactionAsync("suppressed-tag", TestContext.Current.CancellationToken));
            Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
        }

        [Fact]
        public void BeginReadOnlyTransaction_WhenInterceptorSuppressesWithNull_DoesNotCallBeginTransaction()
        {
            var interceptor = new NullSuppressingTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            Assert.Throws<NullReferenceException>(() => context.Database.BeginReadOnlyTransaction());
            Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
        }

        [Fact]
        public async Task BeginReadOnlyTransactionAsync_WhenInterceptorSuppressesWithNull_DoesNotCallBeginTransaction()
        {
            var interceptor = new NullSuppressingTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);

            await Assert.ThrowsAsync<NullReferenceException>(
                () => context.Database.BeginReadOnlyTransactionAsync(TestContext.Current.CancellationToken));
            Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
        }

        [Fact]
        public void BeginTransaction_WhenStartingInterceptorThrows_ClosesConnectionAndRethrows()
        {
            var interceptor = new FailingStartingTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            var exception = Assert.Throws<InvalidOperationException>(() => context.Database.BeginTransaction("tag"));
            Assert.Equal("TransactionStarting failed", exception.Message);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenStartingInterceptorThrows_ClosesConnectionAndRethrows()
        {
            var interceptor = new FailingStartingTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginTransactionAsync("tag", TestContext.Current.CancellationToken));
            Assert.Equal("TransactionStartingAsync failed", exception.Message);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public void BeginReadOnlyTransaction_WhenStartingInterceptorThrows_ClosesConnectionAndRethrows()
        {
            var interceptor = new FailingStartingTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            var exception = Assert.Throws<InvalidOperationException>(() => context.Database.BeginReadOnlyTransaction());
            Assert.Equal("TransactionStarting failed", exception.Message);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginReadOnlyTransactionAsync_WhenStartingInterceptorThrows_ClosesConnectionAndRethrows()
        {
            var interceptor = new FailingStartingTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginReadOnlyTransactionAsync(TestContext.Current.CancellationToken));
            Assert.Equal("TransactionStartingAsync failed", exception.Message);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public void BeginTransaction_WhenStartedInterceptorThrows_DisposesTransactionClosesConnectionAndRethrows()
        {
            var interceptor = new SuppressingAndFailingStartedTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            var exception = Assert.Throws<InvalidOperationException>(() => context.Database.BeginTransaction("tag"));
            Assert.Equal("TransactionStarted failed", exception.Message);
            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenStartedInterceptorThrows_DisposesTransactionClosesConnectionAndRethrows()
        {
            var interceptor = new SuppressingAndFailingStartedTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginTransactionAsync("tag", TestContext.Current.CancellationToken));
            Assert.Equal("TransactionStartedAsync failed", exception.Message);
            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public void BeginReadOnlyTransaction_WhenStartedInterceptorThrows_DisposesTransactionClosesConnectionAndRethrows()
        {
            var interceptor = new SuppressingAndFailingStartedTransactionInterceptor();
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            var exception = Assert.Throws<InvalidOperationException>(() => context.Database.BeginReadOnlyTransaction());
            Assert.Equal("TransactionStarted failed", exception.Message);
            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginReadOnlyTransactionAsync_WhenStartedInterceptorThrows_DisposesTransactionClosesConnectionAndRethrows()
        {
            var interceptor = new SuppressingAndFailingStartedTransactionInterceptor();
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginReadOnlyTransactionAsync(TestContext.Current.CancellationToken));
            Assert.Equal("TransactionStartedAsync failed", exception.Message);
            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public void BeginTransaction_WhenUnderlyingTransactionCreationFails_ClosesConnectionAndRethrows()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();

            Assert.Throws<NotSupportedException>(() => context.Database.BeginTransaction(IsolationLevel.ReadCommitted, "tag"));
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenUnderlyingTransactionCreationFails_ClosesConnectionAndRethrows()
        {
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();

            await Assert.ThrowsAsync<NotSupportedException>(
                () => context.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, "tag", TestContext.Current.CancellationToken));
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public void BeginTransaction_StandardOverload_WhenUnderlyingTransactionCreationFails_ClosesConnectionAndRethrows()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();

            Assert.Throws<NotSupportedException>(() => context.Database.BeginTransaction(IsolationLevel.ReadCommitted));
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginTransactionAsync_StandardOverload_WhenUnderlyingTransactionCreationFails_ClosesConnectionAndRethrows()
        {
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();

            await Assert.ThrowsAsync<NotSupportedException>(
                () => context.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, TestContext.Current.CancellationToken));
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenCancelled_ThrowsOperationCanceledExceptionAndClosesConnection()
        {
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            using var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => context.Database.BeginTransactionAsync("tag", cancellationTokenSource.Token));
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginReadOnlyTransactionAsync_WhenCancelled_ThrowsOperationCanceledExceptionAndClosesConnection()
        {
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            using var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => context.Database.BeginReadOnlyTransactionAsync(cancellationTokenSource.Token));
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public void BeginTransaction_WhenStartedInterceptorThrowsAndDisposeThrows_SuppressesDisposeExceptionAndClosesConnection()
        {
            var interceptor = new SuppressingAndFailingStartedTransactionInterceptor(throwOnDispose: true);
            using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            var exception = Assert.Throws<InvalidOperationException>(
                () => context.Database.BeginTransaction("tag"));
            Assert.Equal("TransactionStarted failed", exception.Message);
            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenStartedInterceptorThrowsAndDisposeThrows_SuppressesDisposeExceptionAndClosesConnection()
        {
            var interceptor = new SuppressingAndFailingStartedTransactionInterceptor(throwOnDispose: true);
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginTransactionAsync("tag", TestContext.Current.CancellationToken));
            Assert.Equal("TransactionStartedAsync failed", exception.Message);
            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenCancelledAfterStartingInterceptorWithSuppression_DisposesSuppressedTransactionAndClosesConnection()
        {
            using var cancellationTokenSource = new CancellationTokenSource();
            var interceptor = new CancellingAndSuppressingStartingTransactionInterceptor(cancellationTokenSource);
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => context.Database.BeginTransactionAsync("tag", cancellationTokenSource.Token));
            Assert.Equal(ConnectionState.Closed, connection.State);
            Assert.NotNull(interceptor.SuppressedTransaction);
            Assert.True(interceptor.SuppressedTransaction.IsDisposed);
        }

        [Fact]
        public void BeginTransaction_WhenConnectionOpenedManually_AndTransactionCreationFails_PreservesOpenConnection()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            context.Database.OpenConnection();
            Assert.Equal(ConnectionState.Open, connection.State);

            Assert.Throws<NotSupportedException>(() => context.Database.BeginTransaction(IsolationLevel.ReadCommitted, "tag"));
            Assert.Equal(ConnectionState.Open, connection.State);

            context.Database.CloseConnection();
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenConnectionOpenedManually_AndTransactionCreationFails_PreservesOpenConnection()
        {
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();
            await context.Database.OpenConnectionAsync(TestContext.Current.CancellationToken);
            Assert.Equal(ConnectionState.Open, connection.State);

            await Assert.ThrowsAsync<NotSupportedException>(
                () => context.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted, "tag", TestContext.Current.CancellationToken));
            Assert.Equal(ConnectionState.Open, connection.State);

            await context.Database.CloseConnectionAsync();
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginTransactionAsync_WhenCancelledDuringStartingInterceptor_ThrowsOperationCanceledExceptionAndClosesConnection()
        {
            using var cancellationTokenSource = new CancellationTokenSource();
            var interceptor = new CancellingStartingTransactionInterceptor(cancellationTokenSource);
            await using var context = new TestTransactionDbContext(ConnectionString, interceptor);
            var connection = context.Database.GetDbConnection();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => context.Database.BeginTransactionAsync("tag", cancellationTokenSource.Token));
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public void BeginTransaction_WhenStandardTransactionActive_ThrowsInvalidOperationException()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            using var transaction = context.Database.BeginTransaction();

            var exception1 = Assert.Throws<InvalidOperationException>(() => context.Database.BeginTransaction("second-tag"));
            Assert.Equal(RelationalStrings.TransactionAlreadyStarted, exception1.Message);

            var exception2 = Assert.Throws<InvalidOperationException>(() => context.Database.BeginReadOnlyTransaction());
            Assert.Equal(RelationalStrings.TransactionAlreadyStarted, exception2.Message);
        }

        [Fact]
        public void BeginTransaction_WhenCustomTransactionActive_StandardBeginTransactionThrowsInvalidOperationException()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            using var transaction = context.Database.BeginTransaction("first-tag");

            var exception = Assert.Throws<InvalidOperationException>(() => context.Database.BeginTransaction());
            Assert.Equal(RelationalStrings.TransactionAlreadyStarted, exception.Message);
        }

        [Fact]
        public void BeginTransaction_WhenAmbientTransactionPresent_ThrowsInvalidOperationException()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            context.Database.OpenConnection();
            using var scope = new TransactionScope();

            var exception = Assert.Throws<InvalidOperationException>(() => context.Database.BeginTransaction("tag"));
            Assert.Equal(RelationalStrings.ConflictingAmbientTransaction, exception.Message);
        }

        [Fact]
        public void BeginReadOnlyTransaction_WhenAmbientTransactionPresent_ThrowsInvalidOperationException()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            context.Database.OpenConnection();
            using var scope = new TransactionScope();

            var exception = Assert.Throws<InvalidOperationException>(() => context.Database.BeginReadOnlyTransaction());
            Assert.Equal(RelationalStrings.ConflictingAmbientTransaction, exception.Message);
        }

        [Fact]
        public void SpannerDatabaseFacadeExtensions_OnNonSpannerDatabase_ThrowsInvalidOperationException()
        {
            using var context = new NonSpannerDbContext(ConnectionString);

            var exception1 = Assert.Throws<InvalidOperationException>(() => context.Database.BeginTransaction("tag"));
            Assert.Equal("Transaction tags can only be used with Spanner databases", exception1.Message);

            var exception2 = Assert.Throws<InvalidOperationException>(() => context.Database.BeginTransaction(IsolationLevel.Serializable, "tag"));
            Assert.Equal("Transaction tags can only be used with Spanner databases", exception2.Message);

            var exception3 = Assert.Throws<InvalidOperationException>(() => context.Database.BeginReadOnlyTransaction());
            Assert.Equal("Read-only transactions can only be started for Spanner databases", exception3.Message);

            var exception4 = Assert.Throws<InvalidOperationException>(() => context.Database.BeginReadOnlyTransaction(TimestampBound.Strong));
            Assert.Equal("Read-only transactions can only be started for Spanner databases", exception4.Message);
        }

        [Fact]
        public async Task SpannerDatabaseFacadeExtensions_OnNonSpannerDatabaseAsync_ThrowsInvalidOperationException()
        {
            await using var context = new NonSpannerDbContext(ConnectionString);

            var exception1 = await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginTransactionAsync("tag", TestContext.Current.CancellationToken));
            Assert.Equal("Transaction tags can only be used with Spanner databases", exception1.Message);

            var exception2 = await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginTransactionAsync(IsolationLevel.Serializable, "tag", TestContext.Current.CancellationToken));
            Assert.Equal("Transaction tags can only be used with Spanner databases", exception2.Message);

            var exception3 = await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginReadOnlyTransactionAsync(TestContext.Current.CancellationToken));
            Assert.Equal("Read-only transactions can only be started for Spanner databases", exception3.Message);

            var exception4 = await Assert.ThrowsAsync<InvalidOperationException>(
                () => context.Database.BeginReadOnlyTransactionAsync(TimestampBound.Strong, TestContext.Current.CancellationToken));
            Assert.Equal("Read-only transactions can only be started for Spanner databases", exception4.Message);
        }

        [Fact]
        public void BeginReadOnlyTransaction_WhenTimestampBoundIsNull_ThrowsArgumentNullException()
        {
            using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();

            Assert.Throws<ArgumentNullException>(() => context.Database.BeginReadOnlyTransaction(null!));
            Assert.Equal(ConnectionState.Closed, connection.State);
        }

        [Fact]
        public async Task BeginReadOnlyTransactionAsync_WhenTimestampBoundIsNull_ThrowsArgumentNullException()
        {
            await using var context = new TestTransactionDbContext(ConnectionString);
            var connection = context.Database.GetDbConnection();

            await Assert.ThrowsAsync<ArgumentNullException>(
                () => context.Database.BeginReadOnlyTransactionAsync(null!, TestContext.Current.CancellationToken));
            Assert.Equal(ConnectionState.Closed, connection.State);
        }
    }
}
