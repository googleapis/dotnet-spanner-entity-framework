// Copyright 2025 Google LLC
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

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib.MockServer;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class DataSourceTests : AbstractMockServerTests
{
    [Test]
    public async Task CreateConnection()
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var connection = dataSource.CreateConnection();
        Assert.That(connection.State, Is.EqualTo(ConnectionState.Closed));

        await connection.OpenAsync();
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task OpenConnection([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var connection = async
            ? await dataSource.OpenConnectionAsync()
            : dataSource.OpenConnection();

        Assert.That(connection.State, Is.EqualTo(ConnectionState.Open));
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteScalarOnConnectionlessCommand([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var command = dataSource.CreateCommand();
        command.CommandText = "SELECT 1";

        if (async)
        {
            Assert.That(await command.ExecuteScalarAsync(), Is.EqualTo(1));
        }
        else
        {
            Assert.That(command.ExecuteScalar(), Is.EqualTo(1));
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteNonQueryOnConnectionlessCommand([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var command = dataSource.CreateCommand();
        command.CommandText = "SELECT 1";

        if (async)
        {
            Assert.That(await command.ExecuteNonQueryAsync(), Is.EqualTo(-1));
        }
        else
        {
            Assert.That(command.ExecuteNonQuery(), Is.EqualTo(-1));
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteReaderOnConnectionlessCommand([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var command = dataSource.CreateCommand();
        command.CommandText = "SELECT 1";

        await using var reader = async ? await command.ExecuteReaderAsync() : command.ExecuteReader();
        Assert.That(reader.Read());
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
    }

    [Ignore("Requires support for batching queries")]
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteScalarOnConnectionlessBatch([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand("SELECT 1");
        batch.AddSpannerBatchCommand("SELECT 2");

        if (async)
        {
            Assert.That(await batch.ExecuteScalarAsync(), Is.EqualTo(1));
        }
        else
        {
            Assert.That(batch.ExecuteScalar(), Is.EqualTo(1));
        }
    }

    [Ignore("Requires support for batching queries")]
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteNonQueryOnConnectionlessBatch([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand("SELECT 1");
        batch.AddSpannerBatchCommand("SELECT 2");

        if (async)
        {
            Assert.That(await batch.ExecuteNonQueryAsync(), Is.EqualTo(-1));
        }
        else
        {
            Assert.That(batch.ExecuteNonQuery(), Is.EqualTo(-1));
        }
    }

    [Ignore("Requires support for batching queries")]
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteReaderOnConnectionlessBatch([Values] bool async)
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand("SELECT 1");
        batch.AddSpannerBatchCommand("SELECT 2");

        await using var reader = async ? await batch.ExecuteReaderAsync() : batch.ExecuteReader();
        Assert.That(reader.Read());
        Assert.That(reader.GetInt32(0), Is.EqualTo(1));
        Assert.That(reader.NextResult());
        Assert.That(reader.Read());
        Assert.That(reader.GetInt32(0), Is.EqualTo(2));
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteScalarOnConnectionlessDmlBatch([Values] bool async)
    {
        const string sql = "insert into my_table (id) values (default)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand(sql);
        batch.AddSpannerBatchCommand(sql);

        if (async)
        {
            await Assert.ThrowsAsync<NotImplementedException>(async () => await batch.ExecuteScalarAsync());
        }
        else
        {
            Assert.Throws<NotImplementedException>(() => batch.ExecuteScalar());
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteNonQueryOnConnectionlessDmlBatch([Values] bool async)
    {
        const string sql = "insert into my_table (id) values (default)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand(sql);
        batch.AddSpannerBatchCommand(sql);

        if (async)
        {
            Assert.That(await batch.ExecuteNonQueryAsync(), Is.EqualTo(2));
        }
        else
        {
            Assert.That(batch.ExecuteNonQuery(), Is.EqualTo(2));
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task ExecuteReaderOnConnectionlessDmlBatch([Values] bool async)
    {
        const string sql = "insert into my_table (id) values (default)";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var batch = dataSource.CreateBatch();
        batch.AddSpannerBatchCommand(sql);
        batch.AddSpannerBatchCommand(sql);

        if (async)
        {
            await Assert.ThrowsAsync<NotImplementedException>(async () => await batch.ExecuteReaderAsync());
        }
        else
        {
            Assert.Throws<NotImplementedException>(() => batch.ExecuteReader());
        }
    }

    [Test]
    public void Dispose()
    {
        var dataSource = SpannerDataSource.Create(ConnectionString);
        var connection1 = dataSource.OpenConnection();
        var connection2 = dataSource.OpenConnection();
        connection1.Close();

        // SpannerDataSource does not contain any state, so disposing it is a no-op.
        dataSource.Dispose();
        using var connection3 = dataSource.OpenConnection();

        connection2.Close();
    }

    [Test]
    public async Task DisposeAsync()
    {
        var dataSource = SpannerDataSource.Create(ConnectionString);
        var connection1 = await dataSource.OpenConnectionAsync();
        var connection2 = await dataSource.OpenConnectionAsync();
        await connection1.CloseAsync();

        // SpannerDataSource does not contain any state, so disposing it is a no-op.
        await dataSource.DisposeAsync();
        await using var connection3 = await dataSource.OpenConnectionAsync();

        await connection2.CloseAsync();
    }

    [Test]
    public async Task CannotAccessConnectionTransactionOnDataSourceCommand()
    {
        await using var command = DataSource.CreateCommand();
    
        Assert.That(() => command.Connection, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => command.Connection = null, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => command.Transaction, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => command.Transaction = null, Throws.Exception.TypeOf<NotSupportedException>());
    
        Assert.That(() => command.Prepare(), Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => command.PrepareAsync(), Throws.Exception.TypeOf<NotSupportedException>());
    }
    
    [Test]
    public async Task CannotAccessConnectionTransactionOnDataSourceBatch()
    {
        await using var batch = DataSource.CreateBatch();
    
        Assert.That(() => batch.Connection, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => batch.Connection = null, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => batch.Transaction, Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => batch.Transaction = null, Throws.Exception.TypeOf<NotSupportedException>());
    
        Assert.That(() => batch.Prepare(), Throws.Exception.TypeOf<NotSupportedException>());
        Assert.That(() => batch.PrepareAsync(), Throws.Exception.TypeOf<NotSupportedException>());
    }
    
    [Test]
    [SuppressMessage("ReSharper", "MethodHasAsyncOverload")]
    public async Task AsDbDataSource([Values] bool async)
    {
        await using DbDataSource dataSource = SpannerDataSource.Create(ConnectionString);
        await using var connection = async
            ? await dataSource.OpenConnectionAsync()
            : dataSource.OpenConnection();
        Assert.That(connection.State, Is.EqualTo(ConnectionState.Open));

        await using var command = dataSource.CreateCommand("SELECT 1");

        Assert.That(async
            ? await command.ExecuteScalarAsync()
            : command.ExecuteScalar(), Is.EqualTo(1));
    }

    [Test]
    public async Task CreateFromBuilder_MutatingBuilderAfterCreate_DoesNotAffectDataSourceOrConnections()
    {
        var builder = new SpannerConnectionStringBuilder(ConnectionString);
        await using var dataSource = SpannerDataSource.Create(builder);

        builder.DataSource = "projects/p1/instances/i1/databases/mutated_db";

        Assert.That(dataSource.ConnectionString, Does.Contain("projects/p1/instances/i1/databases/d1"));
        Assert.That(dataSource.ConnectionString, Does.Not.Contain("mutated_db"));

        await using var connection = await dataSource.OpenConnectionAsync();
        Assert.That(connection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        var executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(1));
        Assert.That(executeRequests[0].Session, Does.StartWith("projects/p1/instances/i1/databases/d1/sessions/"));
    }

    [Test]
    public async Task ChangeDatabase_OnDataSourceConnection_DoesNotAffectDataSourceOrOtherConnections()
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var switchedConnection = await dataSource.OpenConnectionAsync();
        await using var preCreatedClosedConnection = dataSource.CreateConnection();

        switchedConnection.ChangeDatabase("tenant_b");

        Assert.That(switchedConnection.Database, Is.EqualTo("projects/p1/instances/i1/databases/tenant_b"));
        Assert.That(switchedConnection.ConnectionString, Does.Contain("projects/p1/instances/i1/databases/tenant_b"));
        Assert.That(dataSource.ConnectionString, Does.Contain("projects/p1/instances/i1/databases/d1"));
        Assert.That(dataSource.ConnectionString, Does.Not.Contain("tenant_b"));

        Assert.That(preCreatedClosedConnection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
        await preCreatedClosedConnection.OpenAsync();

        await using var newlyCreatedConnection = await dataSource.OpenConnectionAsync();
        Assert.That(newlyCreatedConnection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));

        Assert.That(await switchedConnection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        Assert.That(await preCreatedClosedConnection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        Assert.That(await newlyCreatedConnection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        var executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(3));
        Assert.That(executeRequests[0].Session, Does.StartWith("projects/p1/instances/i1/databases/tenant_b/sessions/"));
        Assert.That(executeRequests[1].Session, Does.StartWith("projects/p1/instances/i1/databases/d1/sessions/"));
        Assert.That(executeRequests[2].Session, Does.StartWith("projects/p1/instances/i1/databases/d1/sessions/"));
    }

    [Test]
    public async Task ChangeDatabase_BeforeOpenAndRepeatedlyOnDataSourceConnection_IsolatesFromDataSource()
    {
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        await using var connection = dataSource.CreateConnection();

        connection.ChangeDatabase("tenant_b");
        connection.ChangeDatabase("tenant_c");
        await connection.OpenAsync();

        Assert.That(connection.Database, Is.EqualTo("projects/p1/instances/i1/databases/tenant_c"));
        Assert.That(dataSource.ConnectionString, Does.Contain("projects/p1/instances/i1/databases/d1"));
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        await using var otherConnection = await dataSource.OpenConnectionAsync();
        Assert.That(await otherConnection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        var executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(2));
        Assert.That(executeRequests[0].Session, Does.StartWith("projects/p1/instances/i1/databases/tenant_c/sessions/"));
        Assert.That(executeRequests[1].Session, Does.StartWith("projects/p1/instances/i1/databases/d1/sessions/"));
    }

    [Test]
    public async Task ChangeDatabase_ConcurrentCallsOnDataSourceConnections_RemainIsolated()
    {
        const int workerCount = 4;
        await using var dataSource = SpannerDataSource.Create(ConnectionString);
        using var barrier = new Barrier(workerCount * 2);

        var switchTasks = Enumerable.Range(0, workerCount).Select(index => Task.Run(async () =>
        {
            await using var connection = await dataSource.OpenConnectionAsync();
            Assert.That(barrier.SignalAndWait(TimeSpan.FromSeconds(10)), Is.True);
            var tenantDatabase = $"tenant_{index}";
            connection.ChangeDatabase(tenantDatabase);
            Assert.That(connection.Database, Is.EqualTo($"projects/p1/instances/i1/databases/{tenantDatabase}"));
            Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
            return connection.Database;
        })).ToArray();

        var defaultTasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(async () =>
        {
            await using var connection = dataSource.CreateConnection();
            Assert.That(barrier.SignalAndWait(TimeSpan.FromSeconds(10)), Is.True);
            await connection.OpenAsync();
            Assert.That(connection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
            Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
            return connection.Database;
        })).ToArray();

        var switchedDatabases = await Task.WhenAll(switchTasks);
        var defaultDatabases = await Task.WhenAll(defaultTasks);

        Assert.That(switchedDatabases, Is.EquivalentTo(new[]
        {
            "projects/p1/instances/i1/databases/tenant_0",
            "projects/p1/instances/i1/databases/tenant_1",
            "projects/p1/instances/i1/databases/tenant_2",
            "projects/p1/instances/i1/databases/tenant_3"
        }));
        Assert.That(defaultDatabases, Is.All.EqualTo("projects/p1/instances/i1/databases/d1"));
        Assert.That(dataSource.ConnectionString, Does.Contain("projects/p1/instances/i1/databases/d1"));

        var executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(workerCount * 2));
        for (var index = 0; index < workerCount; index++)
        {
            var expectedPrefix = $"projects/p1/instances/i1/databases/tenant_{index}/sessions/";
            Assert.That(executeRequests.Count(request => request.Session.StartsWith(expectedPrefix, StringComparison.Ordinal)), Is.EqualTo(1));
        }
        Assert.That(executeRequests.Count(request => request.Session.StartsWith("projects/p1/instances/i1/databases/d1/sessions/", StringComparison.Ordinal)), Is.EqualTo(workerCount));
    }

    [Test]
    public async Task ChangeDatabase_OnProjectInstanceDatabaseDataSource_DoesNotAffectDataSourceOrOtherConnections()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            Host = Fixture.Host,
            Port = (uint)Fixture.Port,
            UsePlainText = true,
            Project = "p1",
            Instance = "i1",
            Database = "d1"
        };
        await using var dataSource = SpannerDataSource.Create(builder);
        await using var switchedConnection = await dataSource.OpenConnectionAsync();
        switchedConnection.ChangeDatabase("tenant_b");

        await using var otherConnection = await dataSource.OpenConnectionAsync();
        Assert.That(switchedConnection.Database, Is.EqualTo("projects/p1/instances/i1/databases/tenant_b"));
        Assert.That(otherConnection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
        Assert.That(dataSource.ConnectionString, Does.Contain("Database=d1"));

        Assert.That(await switchedConnection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        Assert.That(await otherConnection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        var executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(2));
        Assert.That(executeRequests[0].Session, Does.StartWith("projects/p1/instances/i1/databases/tenant_b/sessions/"));
        Assert.That(executeRequests[1].Session, Does.StartWith("projects/p1/instances/i1/databases/d1/sessions/"));
    }
}