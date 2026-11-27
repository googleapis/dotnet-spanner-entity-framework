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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class SpannerPoolTests
{
    [Test]
    public async Task GetDialect_FallsBackToShowDatabaseDialect([Values] bool useAsync)
    {
        var fakeLib = new FakeDialectSpannerLib(statement => statement.Sql switch
        {
            "SHOW VARIABLE DATABASE_DIALECT" => throw new SpannerException(
                new Status { Code = (int)Code.InvalidArgument, Message = "Unknown variable DATABASE_DIALECT" }),
            "SHOW DATABASE_DIALECT" => "POSTGRESQL",
            _ => throw new AssertionException($"Unexpected SQL: {statement.Sql}"),
        });
        var pool = new SpannerPool("fake-dsn", Pool.Create(fakeLib, "fake-dsn"));

        var dialect = useAsync
            ? await pool.GetDialectAsync()
            : pool.GetDialect();

        Assert.That(dialect, Is.SameAs(SpannerDialect.Postgresql));
        Assert.That(fakeLib.ExecutedSql, Is.EqualTo(new[]
        {
            "SHOW VARIABLE DATABASE_DIALECT",
            "SHOW DATABASE_DIALECT",
        }));
    }

    [TestCase("")]
    [TestCase("UNKNOWN_DIALECT")]
    [TestCase(null)]
    public async Task GetDialect_FallbackOnUnrecognizedFirstResult_SucceedsOnSecondQuery(string? firstValue)
    {
        var fakeLib = new FakeDialectSpannerLib(statement => statement.Sql switch
        {
            "SHOW VARIABLE DATABASE_DIALECT" => firstValue,
            "SHOW DATABASE_DIALECT" => "GOOGLE_STANDARD_SQL",
            _ => throw new AssertionException($"Unexpected SQL: {statement.Sql}"),
        });
        var syncPool = new SpannerPool("fake-dsn-sync", Pool.Create(fakeLib, "fake-dsn-sync"));
        Assert.That(syncPool.GetDialect(), Is.SameAs(SpannerDialect.GoogleStandardSql));

        var asyncPool = new SpannerPool("fake-dsn-async", Pool.Create(fakeLib, "fake-dsn-async"));
        Assert.That(await asyncPool.GetDialectAsync(), Is.SameAs(SpannerDialect.GoogleStandardSql));
    }

    [TestCase("")]
    [TestCase("UNKNOWN_DIALECT")]
    [TestCase(null)]
    public async Task GetDialect_EmptyUnknownOrUnsupportedDialect_ThrowsInvalidOperationException(string? returnedValue)
    {
        var fakeLib = new FakeDialectSpannerLib(_ => returnedValue);
        var syncPool = new SpannerPool("fake-dsn-sync", Pool.Create(fakeLib, "fake-dsn-sync"));
        Assert.Throws<InvalidOperationException>(() => syncPool.GetDialect());

        var asyncPool = new SpannerPool("fake-dsn-async", Pool.Create(fakeLib, "fake-dsn-async"));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await asyncPool.GetDialectAsync());
    }

    [Test]
    public async Task GetDialect_RowWithZeroValues_ThrowsInvalidOperationException([Values] bool useAsync)
    {
        var fakeLib = new FakeDialectSpannerLib((_, _) => Task.FromResult<ListValue?>(new ListValue()));
        var pool = new SpannerPool("fake-dsn", Pool.Create(fakeLib, "fake-dsn"));

        if (useAsync)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await pool.GetDialectAsync());
        }
        else
        {
            Assert.Throws<InvalidOperationException>(() => pool.GetDialect());
        }
    }

    [Test]
    public async Task GetDialect_AllDialectQueriesInvalidArgument_ThrowsInvalidOperationException([Values] bool useAsync)
    {
        var fakeLib = new FakeDialectSpannerLib(_ =>
            throw new SpannerException(new Status { Code = (int)Code.InvalidArgument, Message = "Unsupported SHOW" }));
        var pool = new SpannerPool("fake-dsn", Pool.Create(fakeLib, "fake-dsn"));

        if (useAsync)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await pool.GetDialectAsync());
        }
        else
        {
            Assert.Throws<InvalidOperationException>(() => pool.GetDialect());
        }
    }

    [Test]
    public async Task GetDialect_NonInvalidArgumentSpannerException_TranslatesAndDoesNotPoisonCache([Values] bool useAsync)
    {
        var callCount = 0;
        var fakeLib = new FakeDialectSpannerLib(_ =>
        {
            if (Interlocked.Increment(ref callCount) == 1)
            {
                throw new SpannerException(new Status { Code = (int)Code.Unavailable, Message = "Unavailable" });
            }
            return "POSTGRESQL";
        });
        var pool = new SpannerPool("fake-dsn", Pool.Create(fakeLib, "fake-dsn"));

        if (useAsync)
        {
            await Assert.ThrowsAsync<SpannerDbException>(async () => await pool.GetDialectAsync());
            Assert.That(await pool.GetDialectAsync(), Is.SameAs(SpannerDialect.Postgresql));
        }
        else
        {
            Assert.Throws<SpannerDbException>(() => pool.GetDialect());
            Assert.That(pool.GetDialect(), Is.SameAs(SpannerDialect.Postgresql));
        }
    }

    [Test]
    public async Task GetDialect_TransientInvalidOperation_DoesNotPoisonCache([Values] bool useAsync)
    {
        var callCount = 0;
        var fakeLib = new FakeDialectSpannerLib(_ =>
            Interlocked.Increment(ref callCount) <= 2 ? "UNKNOWN" : "GOOGLE_STANDARD_SQL");
        var pool = new SpannerPool("fake-dsn", Pool.Create(fakeLib, "fake-dsn"));

        if (useAsync)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await pool.GetDialectAsync());
            Assert.That(await pool.GetDialectAsync(), Is.SameAs(SpannerDialect.GoogleStandardSql));
        }
        else
        {
            Assert.Throws<InvalidOperationException>(() => pool.GetDialect());
            Assert.That(pool.GetDialect(), Is.SameAs(SpannerDialect.GoogleStandardSql));
        }
    }

    [Test]
    public async Task GetDialect_CancelledSpannerExceptionOrToken_ThrowsOperationCanceledException([Values] bool useAsync)
    {
        var fakeLib = new FakeDialectSpannerLib(_ =>
            throw new SpannerException(new Status { Code = (int)Code.Cancelled, Message = "Cancelled" }));
        var pool = new SpannerPool("fake-dsn", Pool.Create(fakeLib, "fake-dsn"));

        if (useAsync)
        {
            await Assert.ThrowsAsync<OperationCanceledException>(async () => await pool.GetDialectAsync());

            using var cancellationTokenSource = new CancellationTokenSource();
            cancellationTokenSource.Cancel();
            var preCancelledPool = new SpannerPool("fake-dsn-2", Pool.Create(
                new FakeDialectSpannerLib(_ => "GOOGLE_STANDARD_SQL"), "fake-dsn-2"));
            await Assert.CatchAsync<OperationCanceledException>(
                async () => await preCancelledPool.GetDialectAsync(cancellationTokenSource.Token));
        }
        else
        {
            Assert.Throws<OperationCanceledException>(() => pool.GetDialect());
        }
    }

    [Test]
    public async Task GetDialect_ConcurrentCalls_ProbesOnlyOnce([Values] bool useAsync)
    {
        if (useAsync)
        {
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<ListValue?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var fakeLib = new FakeDialectSpannerLib(async (_, token) =>
            {
                entered.TrySetResult();
                return await release.Task.WaitAsync(token).ConfigureAwait(false);
            });
            var pool = new SpannerPool("fake-dsn", Pool.Create(fakeLib, "fake-dsn"));

            var firstTask = pool.GetDialectAsync();
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            var tasks = new Task<SpannerDialect>[8];
            tasks[0] = firstTask;
            for (var index = 1; index < tasks.Length; index++)
            {
                tasks[index] = pool.GetDialectAsync();
            }

            release.SetResult(new ListValue { Values = { Value.ForString("GOOGLE_STANDARD_SQL") } });
            var dialects = await Task.WhenAll(tasks);

            Assert.That(dialects, Has.All.SameAs(SpannerDialect.GoogleStandardSql));
            Assert.That(await pool.GetDialectAsync(), Is.SameAs(SpannerDialect.GoogleStandardSql));
            Assert.That(fakeLib.ExecutedSql, Is.EqualTo(new[] { "SHOW VARIABLE DATABASE_DIALECT" }));
        }
        else
        {
            using var entered = new ManualResetEventSlim(false);
            using var release = new ManualResetEventSlim(false);
            var fakeLib = new FakeDialectSpannerLib(_ =>
            {
                entered.Set();
                if (!release.Wait(TimeSpan.FromSeconds(5)))
                {
                    throw new TimeoutException("Timed out waiting for concurrent dialect probe release.");
                }
                return "GOOGLE_STANDARD_SQL";
            });
            var pool = new SpannerPool("fake-dsn", Pool.Create(fakeLib, "fake-dsn"));

            var firstTask = Task.Run(() => pool.GetDialect());
            Assert.That(entered.Wait(TimeSpan.FromSeconds(5)), Is.True);

            var tasks = new Task<SpannerDialect>[8];
            tasks[0] = firstTask;
            for (var index = 1; index < tasks.Length; index++)
            {
                tasks[index] = Task.Run(() => pool.GetDialect());
            }

            release.Set();
            var dialects = await Task.WhenAll(tasks);

            Assert.That(dialects, Has.All.SameAs(SpannerDialect.GoogleStandardSql));
            Assert.That(pool.GetDialect(), Is.SameAs(SpannerDialect.GoogleStandardSql));
            Assert.That(fakeLib.ExecutedSql, Is.EqualTo(new[] { "SHOW VARIABLE DATABASE_DIALECT" }));
        }
    }

    [Test]
    public async Task GetDialect_FallbackQueryNonInvalidArgumentSpannerException_TranslatesAndDoesNotPoisonCache([Values] bool useAsync)
    {
        var fallbackCalls = 0;
        var fakeLib = new FakeDialectSpannerLib(statement =>
        {
            if (statement.Sql == "SHOW VARIABLE DATABASE_DIALECT")
            {
                throw new SpannerException(new Status { Code = (int)Code.InvalidArgument, Message = "Unknown variable" });
            }
            if (Interlocked.Increment(ref fallbackCalls) == 1)
            {
                throw new SpannerException(new Status { Code = (int)Code.Unavailable, Message = "Temporary failure" });
            }
            return "GOOGLE_STANDARD_SQL";
        });
        var pool = new SpannerPool("fake-dsn", Pool.Create(fakeLib, "fake-dsn"));

        if (useAsync)
        {
            await Assert.ThrowsAsync<SpannerDbException>(async () => await pool.GetDialectAsync());
            Assert.That(await pool.GetDialectAsync(), Is.SameAs(SpannerDialect.GoogleStandardSql));
        }
        else
        {
            Assert.Throws<SpannerDbException>(() => pool.GetDialect());
            Assert.That(pool.GetDialect(), Is.SameAs(SpannerDialect.GoogleStandardSql));
        }
    }

    [Test]
    public async Task GetDialectAsync_WaiterCancelledWhileProbeInFlight_ThrowsAndDoesNotBlockOtherWaiters()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource<ListValue?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var fakeLib = new FakeDialectSpannerLib(async (_, token) =>
        {
            entered.TrySetResult();
            return await release.Task.WaitAsync(token).ConfigureAwait(false);
        });
        var pool = new SpannerPool("fake-dsn", Pool.Create(fakeLib, "fake-dsn"));

        var inFlightTask = pool.GetDialectAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using var cancellationTokenSource = new CancellationTokenSource();
        var waiterTask = pool.GetDialectAsync(cancellationTokenSource.Token);
        cancellationTokenSource.Cancel();
        await Assert.CatchAsync<OperationCanceledException>(async () => await waiterTask);

        release.SetResult(new ListValue { Values = { Value.ForString("GOOGLE_STANDARD_SQL") } });
        Assert.That(await inFlightTask, Is.SameAs(SpannerDialect.GoogleStandardSql));
        Assert.That(fakeLib.ExecutedSql, Is.EqualTo(new[] { "SHOW VARIABLE DATABASE_DIALECT" }));
    }

    private sealed class FakeDialectSpannerLib(Func<ExecuteSqlRequest, CancellationToken, Task<ListValue?>> asyncHandler) : ISpannerLib
    {
        private readonly object _syncRoot = new();
        private readonly Dictionary<long, ListValue?> _rowValues = [];
        private long _nextId = 1;

        public FakeDialectSpannerLib(Func<ExecuteSqlRequest, string?> handler)
            : this((statement, _) =>
            {
                var value = handler(statement);
                return Task.FromResult(value == null ? null : new ListValue { Values = { Value.ForString(value) } });
            })
        {
        }

        public List<string> ExecutedSql { get; } = [];

        public Pool CreatePool(string connectionString) => new(this, Interlocked.Increment(ref _nextId));

        public void ClosePool(Pool pool)
        {
        }

        public Connection CreateConnection(Pool pool) => new(pool, Interlocked.Increment(ref _nextId));

        public void CloseConnection(Connection connection)
        {
        }

        public Rows Execute(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0)
        {
            RecordExecutedSql(statement.Sql);
            var row = asyncHandler(statement, CancellationToken.None).GetAwaiter().GetResult();
            return CreateRows(connection, row);
        }

        public async Task<Rows> ExecuteAsync(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RecordExecutedSql(statement.Sql);
            var row = await asyncHandler(statement, cancellationToken).ConfigureAwait(false);
            return CreateRows(connection, row);
        }

        private void RecordExecutedSql(string sql)
        {
            lock (_syncRoot)
            {
                ExecutedSql.Add(sql);
            }
        }

        private Rows CreateRows(Connection connection, ListValue? row)
        {
            var id = Interlocked.Increment(ref _nextId);
            lock (_syncRoot)
            {
                _rowValues[id] = row;
            }
            return new Rows(connection, id, initMetadata: false);
        }

        public ResultSetMetadata? Metadata(Rows rows) => new()
        {
            RowType = new StructType
            {
                Fields = { new StructType.Types.Field { Name = "DATABASE_DIALECT", Type = new V1.Type { Code = TypeCode.String } } },
            },
        };

        public Task<ResultSetMetadata?> MetadataAsync(Rows rows, CancellationToken cancellationToken = default) =>
            Task.FromResult(Metadata(rows));

        public ListValue? Next(Rows rows, int numRows, ISpannerLib.RowEncoding encoding)
        {
            lock (_syncRoot)
            {
                return _rowValues.Remove(rows.Id, out var row) ? row : null;
            }
        }

        public Task<ListValue?> NextAsync(Rows rows, int numRows, ISpannerLib.RowEncoding encoding, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Next(rows, numRows, encoding));
        }

        public void CloseRows(Rows rows)
        {
        }

        public ResultSetStats? Stats(Rows rows) => null;

        public ResultSetMetadata? NextResultSet(Rows rows) => null;

        public Task<ResultSetMetadata?> NextResultSetAsync(Rows rows, CancellationToken cancellationToken = default) =>
            Task.FromResult<ResultSetMetadata?>(null);

        public CommitResponse? WriteMutations(Connection connection, BatchWriteRequest.Types.MutationGroup mutations) =>
            throw new NotImplementedException();

        public Task<CommitResponse?> WriteMutationsAsync(Connection connection, BatchWriteRequest.Types.MutationGroup mutations, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public long[] ExecuteBatch(Connection connection, ExecuteBatchDmlRequest statements) =>
            throw new NotImplementedException();

        public Task<long[]> ExecuteBatchAsync(Connection connection, ExecuteBatchDmlRequest statements, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public void BeginTransaction(Connection connection, TransactionOptions transactionOptions) =>
            throw new NotImplementedException();

        public Task BeginTransactionAsync(Connection connection, TransactionOptions transactionOptions, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public CommitResponse? Commit(Connection connection) =>
            throw new NotImplementedException();

        public Task<CommitResponse?> CommitAsync(Connection connection, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public void Rollback(Connection connection) =>
            throw new NotImplementedException();

        public Task RollbackAsync(Connection connection, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();
    }
}
