using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;
using Google.Protobuf.WellKnownTypes;
using NUnit.Framework;

namespace Google.Cloud.Spanner.DataProvider.Tests;

[TestFixture]
public class RowsTests
{
    [Test]
    public async Task TestGetStatsAsyncDoesNotLeakSyncStatsCall()
    {
        var stubSpanner = new StubSpannerLib();
        var pool = new Pool(stubSpanner, 1L);
        var connection = new Connection(pool, 1L);
        var rows = new Rows(connection, 1L, initMetadata: false);

        // 1. Load stats asynchronously
        var stats = await rows.GetStatsAsync();
        Assert.That(stats, Is.Not.Null);
        Assert.That(stats!.RowCountExact, Is.EqualTo(5L));
        Assert.That(stubSpanner.StatsAsyncCalled, Is.True);
        Assert.That(stubSpanner.StatsCalled, Is.False);

        // 2. Execute Next() synchronously (simulating end of row iteration)
        // This will call Spanner.Next (returns null) and should NOT trigger Spanner.Stats()
        var nextRow = rows.Next();
        Assert.That(nextRow, Is.Null);
        
        // Assert that the synchronous Spanner.Stats was NEVER called!
        Assert.That(stubSpanner.StatsCalled, Is.False, "Synchronous Stats() was called during Next() causing sync-over-async block!");
    }

    [Test]
    public void TestNextResultSetClearsStatsCache()
    {
        var stubSpanner = new StubSpannerLib();
        var pool = new Pool(stubSpanner, 1L);
        var connection = new Connection(pool, 1L);
        var rows = new Rows(connection, 1L, initMetadata: false);

        // 1. Fetch Stats synchronously
        var stats1 = rows.UpdateCount;
        Assert.That(stubSpanner.StatsCount, Is.EqualTo(1));
        
        // 2. Fetch Stats again - should return cached
        var stats2 = rows.UpdateCount;
        Assert.That(stubSpanner.StatsCount, Is.EqualTo(1));

        // 3. Move to next result set (mock NextResultSet to return non-null metadata)
        stubSpanner.NextResultSetMetadata = new ResultSetMetadata();
        var hasNext = rows.NextResultSet();
        Assert.That(hasNext, Is.True);

        // 4. Fetch Stats again - cache should be cleared and it should call Stats() again
        var stats3 = rows.UpdateCount;
        Assert.That(stubSpanner.StatsCount, Is.EqualTo(2));
    }

    [Test]
    public async Task TestNextResultSetAsyncClearsStatsCache()
    {
        var stubSpanner = new StubSpannerLib();
        var pool = new Pool(stubSpanner, 1L);
        var connection = new Connection(pool, 1L);
        var rows = new Rows(connection, 1L, initMetadata: false);

        // 1. Fetch Stats asynchronously
        var stats1 = await rows.GetStatsAsync();
        Assert.That(stubSpanner.StatsAsyncCount, Is.EqualTo(1));
        
        // 2. Fetch Stats again - should return cached
        var stats2 = await rows.GetStatsAsync();
        Assert.That(stubSpanner.StatsAsyncCount, Is.EqualTo(1));

        // 3. Move to next result set asynchronously
        stubSpanner.NextResultSetMetadata = new ResultSetMetadata();
        var hasNext = await rows.NextResultSetAsync();
        Assert.That(hasNext, Is.True);

        // 4. Fetch Stats again - cache should be cleared and it should call StatsAsync() again
        var stats3 = await rows.GetStatsAsync();
        Assert.That(stubSpanner.StatsAsyncCount, Is.EqualTo(2));
    }

    private class StubSpannerLib : ISpannerLib
    {
        public int StatsCount { get; private set; }
        public int StatsAsyncCount { get; private set; }
        public bool StatsCalled => StatsCount > 0;
        public bool StatsAsyncCalled => StatsAsyncCount > 0;
        public ResultSetMetadata? NextResultSetMetadata { get; set; }

        public ResultSetStats? Stats(Rows rows)
        {
            StatsCount++;
            return new ResultSetStats { RowCountExact = 5L };
        }

        public Task<ResultSetStats?> StatsAsync(Rows rows, CancellationToken cancellationToken = default)
        {
            StatsAsyncCount++;
            return Task.FromResult<ResultSetStats?>(new ResultSetStats { RowCountExact = 5L });
        }

        public ListValue? Next(Rows rows, int numRows, ISpannerLib.RowEncoding encoding) => null;

        public Task<ListValue?> NextAsync(Rows rows, int numRows, ISpannerLib.RowEncoding encoding, CancellationToken cancellationToken = default) =>
            Task.FromResult<ListValue?>(null);

        // Stub out other interface methods to satisfy compiler
        public Pool CreatePool(string connectionString) => throw new NotImplementedException();
        public void ClosePool(Pool pool) {}
        public Connection CreateConnection(Pool pool) => throw new NotImplementedException();
        public void CloseConnection(Connection connection) {}
        public Task CloseConnectionAsync(Connection connection, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public CommitResponse? WriteMutations(Connection connection, BatchWriteRequest.Types.MutationGroup mutations) => throw new NotImplementedException();
        public Task<CommitResponse?> WriteMutationsAsync(Connection connection, BatchWriteRequest.Types.MutationGroup mutations, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Rows Execute(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0) => throw new NotImplementedException();
        public Task<Rows> ExecuteAsync(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public long[] ExecuteBatch(Connection connection, ExecuteBatchDmlRequest statements) => throw new NotImplementedException();
        public Task<long[]> ExecuteBatchAsync(Connection connection, ExecuteBatchDmlRequest statements, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public ResultSetMetadata? Metadata(Rows rows) => null;
        public Task<ResultSetMetadata?> MetadataAsync(Rows rows, CancellationToken cancellationToken = default) => Task.FromResult<ResultSetMetadata?>(null);
        public ResultSetMetadata? NextResultSet(Rows rows) => NextResultSetMetadata;
        public Task<ResultSetMetadata?> NextResultSetAsync(Rows rows, CancellationToken cancellationToken = default) => Task.FromResult(NextResultSetMetadata);
        public void CloseRows(Rows rows) {}
        public Task CloseRowsAsync(Rows rows, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public void BeginTransaction(Connection connection, TransactionOptions transactionOptions) {}
        public Task BeginTransactionAsync(Connection connection, TransactionOptions transactionOptions, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public CommitResponse? Commit(Connection connection) => throw new NotImplementedException();
        public Task<CommitResponse?> CommitAsync(Connection connection, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public void Rollback(Connection connection) {}
        public Task RollbackAsync(Connection connection, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public void Dispose() {}
    }
}
