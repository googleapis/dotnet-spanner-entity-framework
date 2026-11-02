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
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests;

#pragma warning disable EF1001 // Internal EF Core API usage.
public class SynchronizationContextDeadlockTests : IClassFixture<SpannerMockServerFixture>
{
    private readonly SpannerMockServerFixture _fixture;

    public SynchronizationContextDeadlockTests(SpannerMockServerFixture fixture)
    {
        _fixture = fixture;
        ResetMockServer();
    }

    private void ResetMockServer()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            "UPDATE Foo SET Bar='bar' WHERE Id=1",
            StatementResult.CreateUpdateCount(1));
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            "CREATE TABLE TestTable (Id INT64) PRIMARY KEY (Id)",
            StatementResult.CreateUpdateCount(0));
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            @"
                    SELECT CASE WHEN COUNT(*) = 0 THEN FALSE ELSE TRUE END
                    FROM information_schema.tables AS t
                    WHERE t.table_catalog = '' and t.table_schema = ''
                ",
            StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>> { Tuple.Create(V1.TypeCode.Bool, "result") },
                new List<object[]> { new object[] { false } }));
    }

    private string ConnectionString =>
        $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

    private SpannerRetriableConnection CreateConnection()
    {
        var builder = new SpannerConnectionStringBuilder(ConnectionString, ChannelCredentials.Insecure);
        return new SpannerRetriableConnection(new SpannerConnection(builder));
    }

    private class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions options)
            : base(options)
        {
        }
    }

    private TestDbContext CreateDbContext()
    {
        var serviceProvider = SpannerTestHelpers.Instance.CreateServiceProvider();
        var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>()
            .UseInternalServiceProvider(serviceProvider)
            .UseSpanner(ConnectionString, options =>
            {
                SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false);
            }, ChannelCredentials.Insecure);
        return new TestDbContext(optionsBuilder.Options);
    }

    [Fact]
    public void BeginTransaction_Sync_DoesNotDeadlockUnderSynchronizationContext()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();

            using var transaction = connection.BeginTransaction();
            Assert.NotNull(transaction);
            Assert.Equal(IsolationLevel.Unspecified, transaction.IsolationLevel);

            using var serializableTransaction = connection.BeginTransaction(IsolationLevel.Serializable);
            Assert.NotNull(serializableTransaction);
            Assert.Equal(IsolationLevel.Serializable, serializableTransaction.IsolationLevel);
        });
    }

    [Fact]
    public void BeginTransaction_WithTag_Sync_DoesNotDeadlockUnderSynchronizationContext()
    {
        ResetMockServer();
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();

            using var transaction = connection.BeginTransaction(IsolationLevel.Serializable, "custom-tag");
            Assert.NotNull(transaction);
            Assert.Equal(IsolationLevel.Serializable, transaction.IsolationLevel);

            using var command = connection.CreateSelectCommand("SELECT 1");
            command.Transaction = transaction;
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());

            var sqlRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(), request => request.Sql == "SELECT 1");
            Assert.Equal("custom-tag", sqlRequest.RequestOptions?.TransactionTag);
        });
    }

    [Fact]
    public void BeginTransaction_WithTag_Async_DoesNotDeadlockUnderSynchronizationContext()
    {
        ResetMockServer();
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();

            using var transaction = connection.BeginTransactionAsync(IsolationLevel.Serializable, "custom-tag-async").GetAwaiter().GetResult();
            Assert.NotNull(transaction);
            Assert.Equal(IsolationLevel.Serializable, transaction.IsolationLevel);

            using var command = connection.CreateSelectCommand("SELECT 1");
            command.Transaction = transaction;
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());

            var sqlRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(), request => request.Sql == "SELECT 1");
            Assert.Equal("custom-tag-async", sqlRequest.RequestOptions?.TransactionTag);
        });
    }

    [Fact]
    public void ExecuteNonQuery_Dml_Sync_WithoutTransaction_DoesNotDeadlockUnderSynchronizationContext()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();

            using var command = connection.CreateDmlCommand("UPDATE Foo SET Bar='bar' WHERE Id=1");
            var affectedRows = command.ExecuteNonQuery();
            Assert.Equal(1, affectedRows);
        });
    }

    [Fact]
    public void ExecuteNonQuery_Ddl_Sync_WithoutTransaction_DoesNotDeadlockUnderSynchronizationContext()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();

            using var command = connection.CreateDdlCommand("CREATE TABLE TestTable (Id INT64) PRIMARY KEY (Id)");
            var affectedRows = command.ExecuteNonQuery();
            Assert.Equal(0, affectedRows);
        });
    }

    [Fact]
    public void ExecuteNonQuery_Ddl_Async_WithoutTransaction_DoesNotDeadlockUnderSynchronizationContext()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();

            using var command = connection.CreateDdlCommand("CREATE TABLE TestTable (Id INT64) PRIMARY KEY (Id)");
            var affectedRows = command.ExecuteNonQueryAsync().GetAwaiter().GetResult();
            Assert.Equal(0, affectedRows);
        });
    }

    [Fact]
    public void ExecuteDbDataReader_WithTimestampBound_Sync_DoesNotDeadlockUnderSynchronizationContext()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();

            using var command = connection.CreateSelectCommand("SELECT 1");
            command.TimestampBound = TimestampBound.Strong;
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(1L, reader.GetInt64(0));
        });
    }

    [Fact]
    public void DatabaseCreator_Exists_Sync_DoesNotDeadlockUnderSynchronizationContext()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var dbContext = CreateDbContext();
            var creator = Assert.IsAssignableFrom<RelationalDatabaseCreator>(dbContext.Database.GetService<IDatabaseCreator>());
            var exists = creator.Exists();
            Assert.True(exists);
        });
    }

    [Fact]
    public void DatabaseFacade_CanConnect_Sync_DoesNotDeadlockUnderSynchronizationContext()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var dbContext = CreateDbContext();
            var canConnect = dbContext.Database.CanConnect();
            Assert.True(canConnect);
        });
    }


    [Fact]
    public void BeginTransactionAsync_UnderSynchronizationContext_DoesNotDeadlock()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();

            using var transaction = connection.BeginTransactionAsync(IsolationLevel.Serializable).GetAwaiter().GetResult();
            Assert.NotNull(transaction);
            Assert.Equal(IsolationLevel.Serializable, transaction.IsolationLevel);
        });
    }

    [Fact]
    public void BeginReadOnlyTransactionAsync_UnderSynchronizationContext_DoesNotDeadlock()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();

            using var transaction = connection.BeginReadOnlyTransactionAsync(TimestampBound.Strong).GetAwaiter().GetResult();
            Assert.NotNull(transaction);
        });
    }

    [Fact]
    public void ExecuteNonQuery_WithActiveTransaction_UnderSynchronizationContext_DoesNotDeadlock()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginTransaction();

            using var command = connection.CreateDmlCommand("UPDATE Foo SET Bar='bar' WHERE Id=1");
            command.Transaction = transaction;
            var affectedRows = command.ExecuteNonQuery();
            Assert.Equal(1, affectedRows);
        });
    }


    [Fact]
    public void ReadOnlyTransaction_ExecuteDbDataReader_UnderSynchronizationContext_DoesNotDeadlock()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginReadOnlyTransaction(TimestampBound.Strong);

            using var command = connection.CreateSelectCommand("SELECT 1");
            command.Transaction = transaction;
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(1L, reader.GetInt64(0));
        });
    }

    [Fact]
    public void ReadOnlyTransaction_ExecuteDbDataReaderAsync_UnderSynchronizationContext_DoesNotDeadlock()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var connection = CreateConnection();
            connection.Open();
            using var transaction = connection.BeginReadOnlyTransaction(TimestampBound.Strong);

            using var command = connection.CreateSelectCommand("SELECT 1");
            command.Transaction = transaction;
            using var reader = command.ExecuteReaderAsync().GetAwaiter().GetResult();
            Assert.True(reader.Read());
            Assert.Equal(1L, reader.GetInt64(0));
        });
    }

    [Fact]
    public void DbConnection_BeginTransaction_SyncAndAsync_UnderSynchronizationContext_DoesNotDeadlock()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using DbConnection connection = CreateConnection();
            connection.Open();

            using var syncTransaction = connection.BeginTransaction();
            Assert.NotNull(syncTransaction);

            using var asyncTransaction = connection.BeginTransactionAsync().GetAwaiter().GetResult();
            Assert.NotNull(asyncTransaction);
        });
    }

    [Fact]
    public void DatabaseCreator_HasTablesAsync_UnderSynchronizationContext_DoesNotDeadlock()
    {
        using var context = new SingleThreadSynchronizationContext();
        context.Execute(() =>
        {
            using var dbContext = CreateDbContext();
            var creator = Assert.IsAssignableFrom<RelationalDatabaseCreator>(dbContext.Database.GetService<IDatabaseCreator>());
            var hasTables = creator.HasTablesAsync().GetAwaiter().GetResult();
            Assert.False(hasTables);
        });
    }

    /// <summary>
    /// A single-threaded <see cref="SynchronizationContext"/> simulating environments such as Windows Forms,
    /// WPF, or Blazor, where continuations posted without ConfigureAwait(false) must execute on the single
    /// thread and will deadlock if that thread is synchronously blocked.
    /// </summary>
    private sealed class SingleThreadSynchronizationContext : SynchronizationContext, IDisposable
    {
        private readonly BlockingCollection<(SendOrPostCallback Callback, object State)> _workQueue = new();
        private readonly Thread _workerThread;

        public SingleThreadSynchronizationContext()
        {
            _workerThread = new Thread(RunLoop) { IsBackground = true };
            _workerThread.Start();
        }

        public override void Post(SendOrPostCallback callback, object state)
        {
            try
            {
                if (!_workQueue.IsAddingCompleted)
                {
                    _workQueue.Add((callback, state));
                }
            }
            catch (InvalidOperationException)
            {
                // Queue has been marked as complete for adding during tear down.
            }
        }

        public override void Send(SendOrPostCallback callback, object state)
        {
            if (Thread.CurrentThread == _workerThread)
            {
                callback(state);
                return;
            }

            var completionEvent = new ManualResetEventSlim(false);
            Exception capturedException = null;
            _workQueue.Add((_ =>
            {
                try
                {
                    callback(state);
                }
                catch (Exception exception)
                {
                    capturedException = exception;
                }
                finally
                {
                    completionEvent.Set();
                }
            }, null));

            var completed = completionEvent.Wait(TimeSpan.FromSeconds(10));
            Assert.True(completed, "Operation deadlocked or timed out waiting for work item execution on SynchronizationContext");
            if (capturedException != null)
            {
                ExceptionDispatchInfo.Capture(capturedException).Throw();
            }
        }

        private void RunLoop()
        {
            SetSynchronizationContext(this);
            foreach (var (callback, state) in _workQueue.GetConsumingEnumerable())
            {
                callback(state);
            }
        }

        public void Execute(Action action)
        {
            Send(_ => action(), null);
        }

        public void Dispose()
        {
            _workQueue.CompleteAdding();
            _workerThread.Join(TimeSpan.FromSeconds(5));
        }
    }
}
