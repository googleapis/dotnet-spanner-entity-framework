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
using System.Diagnostics.CodeAnalysis;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;
using Google.Cloud.SpannerLib.MockServer;
using Google.Rpc;
using Grpc.Core;
using Status = Grpc.Core.Status;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class ConnectionTests : AbstractMockServerTests
{
    [Test]
    public void TestOpenConnection()
    {
        var connection = new SpannerConnection { ConnectionString = ConnectionString };
        connection.Open();
        connection.Close();
    }

    [Test]
    public void TestExecute()
    {
        var sql = "update all_types set col_float8=1 where col_bigint=1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        var command = connection.CreateCommand();
        command.CommandText = sql;
        var updateCount = command.ExecuteNonQuery();
        Assert.That(updateCount, Is.EqualTo(1));
    }

    [Test]
    public void TestQuery()
    {
        var sql = "select col_varchar from all_types where col_varchar is not null limit 10";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(new V1.Type{Code = TypeCode.String}, "col_varchar", "value1", "value2", "value3"));
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var rowCount = 0;
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                rowCount++;
                Assert.That(reader.GetString(0), Is.EqualTo($"value{rowCount}"));
            }
        }
        Assert.That(rowCount, Is.EqualTo(3));
    }

    [Test]
    public void TestParameterizedQuery()
    {
        var sql = "select * from all_types where col_varchar=@p1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add("2de7b24590e00a58fa7358c9531301c5");
        using (var reader = command.ExecuteReader())
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.Not.Null);
            }
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    Assert.That(reader.GetValue(i), Is.Not.Null);
                }
            }
        }
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests, Has.Count.EqualTo(1));
        var request = requests.First();
        Assert.That(request.Params.Fields, Has.Count.EqualTo(1));
    }

    [Test]
    public void TestTransaction()
    {
        var sql = "select * from all_types where col_varchar=$1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.Parameters.Add("2de7b24590e00a58fa7358c9531301c5");
        using (var reader = command.ExecuteReader())
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.Not.Null);
            }
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    Assert.That(reader.GetValue(i), Is.Not.Null);
                }
            }
        }
        transaction.Commit();
    }
    
    [Test]
    public void TestDisableInternalRetries()
    {
        var sql = "update my_table set value=@p1 where id=@p2 and version=1";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1));
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString + ";retryAbortsInternally=false";
        connection.Open();
        
        using var transaction = connection.BeginTransaction();
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.Parameters.Add("2de7b24590e00a58fa7358c9531301c5");
        command.Parameters.Add(1L);
        command.ExecuteNonQuery();
        Fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(Fixture.SpannerMock.Commit), ExecutionTime.CreateException(StatusCode.Aborted, "Transaction was aborted"));
        Assert.Throws<SpannerException>(transaction.Commit);
        
        var requests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(requests.Count, Is.EqualTo(1));
    }

    [Test]
    public void TestBatchDml()
    {
        var sql1 = "update all_types set col_float8=1 where col_bigint=1";
        var sql2 = "update all_types set col_float8=2 where col_bigint=2";
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql1, StatementResult.CreateUpdateCount(2));
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql2, StatementResult.CreateUpdateCount(3));
        using var connection = new SpannerConnection();
        connection.ConnectionString = ConnectionString;
        connection.Open();
        
        using var command1 = connection.CreateCommand();
        command1.CommandText = sql1;
        using var command2 = connection.CreateCommand();
        command2.CommandText = sql2;
        var affected = connection.ExecuteBatchDml([command1, command2]);
        Assert.That(affected, Is.EqualTo(new long[] { 2, 3 }));
    }
    
    [Test]
    public async Task TestBasicLifecycle()
    {
        await using var conn = new SpannerConnection();
        conn.ConnectionString = ConnectionString;

        var eventConnecting = false;
        var eventOpen = false;
        var eventClosed = false;

        conn.StateChange += (_, e) =>
        {
            if (e is { OriginalState: ConnectionState.Closed, CurrentState: ConnectionState.Connecting })
                eventConnecting = true;
            
            if (e is { OriginalState: ConnectionState.Connecting, CurrentState: ConnectionState.Open })
                eventOpen = true;

            if (e is { OriginalState: ConnectionState.Open, CurrentState: ConnectionState.Closed })
                eventClosed = true;
        };

        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
        Assert.That(eventConnecting, Is.False);
        Assert.That(eventOpen, Is.False);

        await conn.OpenAsync();

        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
        Assert.That(eventConnecting, Is.True);
        Assert.That(eventOpen, Is.True);

        await using (var cmd = new SpannerCommand("SELECT 1", conn))
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            await reader.ReadAsync();
            Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
        }

        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));

        await conn.CloseAsync();

        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
        Assert.That(eventClosed, Is.True);
    }

    [Test]
    [Ignore("SpannerLib should support a connect_timeout property to make this test quicker")]
    public async Task TestInvalidHost()
    {
        await using var conn = new SpannerConnection();
        conn.ConnectionString = $"{Fixture.Host}_invalid:{Fixture.Port}/projects/p1/instances/i1/databases/d1;UsePlainText=true";
        var exception = Assert.Throws<SpannerException>(() => conn.Open());
        Assert.That(exception.Code, Is.EqualTo(Code.DeadlineExceeded));
    }
    
    [Test]
    public async Task TestInvalidDatabase()
    {
        // Close all current pools to ensure that we get a fresh pool.
        SpannerPool.CloseSpannerLib();
        // TODO: Make this a public property in the mock server.
        const string detectDialectQuery =
            "select option_value from information_schema.database_options where option_name='database_dialect'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(detectDialectQuery, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Database not found"))));
        await using var conn = new SpannerConnection();
        conn.ConnectionString = ConnectionString;
        var exception = Assert.Throws<SpannerException>(() => conn.Open());
        Assert.That(exception.Code, Is.EqualTo(Code.NotFound));
    }

    [Test]
    public void TestConnectWithConnectionStringBuilder()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            DataSource = "projects/my-project/instances/my-instance/databases/my-database",
            Host = Fixture.Host,
            Port = (uint) Fixture.Port,
            UsePlainText = true
        };
        using var connection = new SpannerConnection(builder);
        Assert.That(connection.ConnectionString, Is.EqualTo(builder.ConnectionString));
    }

    [Test]
    public void RequiredConnectionStringProperties()
    {
        using var connection = new SpannerConnection();
        Assert.Throws<ArgumentException>(() => connection.ConnectionString = "Host=localhost;Port=80");
    }

    [Test]
    public void FailedConnectThenSucceed()
    {
        // Close all current pools to ensure that we get a fresh pool.
        SpannerPool.CloseSpannerLib();
        // TODO: Make this a public property in the mock server.
        const string detectDialectQuery =
            "select option_value from information_schema.database_options where option_name='database_dialect'";
        Fixture.SpannerMock.AddOrUpdateStatementResult(detectDialectQuery, StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Database not found"))));
        using var conn = new SpannerConnection();
        conn.ConnectionString = ConnectionString;
        var exception = Assert.Throws<SpannerException>(() => conn.Open());
        Assert.That(exception.Code, Is.EqualTo(Code.NotFound));
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Closed));
        
        // Remove the error and retry.
        Fixture.SpannerMock.AddOrUpdateStatementResult(detectDialectQuery, StatementResult.CreateResultSet(new List<Tuple<TypeCode, string>>
        {
            Tuple.Create<TypeCode, string>(TypeCode.String, "option_value")
        }, new List<object[]>
        {
            new object[] { "GOOGLE_STANDARD_SQL" }
        }));
        conn.Open();
        Assert.That(conn.State, Is.EqualTo(ConnectionState.Open));
    }

    [Test]
    [Ignore("Needs connect_timeout property")]
    public void OpenTimeout()
    {
        // Close all current pools to ensure that we get a fresh pool.
        SpannerPool.CloseSpannerLib();
        Fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(Fixture.SpannerMock.CreateSession), ExecutionTime.FromMillis(20, 0));
        var builder = new SpannerConnectionStringBuilder
        {
            Host = Fixture.Host,
            Port = (uint) Fixture.Port,
            UsePlainText = true,
            DataSource = "projects/project1/instances/instance1/databases/database1",
            ConnectionTimeout = 1,
        };
        using var connection = new SpannerConnection();
        connection.ConnectionString = builder.ConnectionString;
        var exception = Assert.Throws<SpannerDbException>(() => connection.Open());
        Assert.That(exception.ErrorCode, Is.EqualTo((int) Code.DeadlineExceeded));
    }

    [Test]
    [Ignore("OpenAsync must be implemented")]
    public async Task OpenCancel()
    {
        // Close all current pools to ensure that we get a fresh pool.
        SpannerPool.CloseSpannerLib();
        Fixture.SpannerMock.AddOrUpdateExecutionTime(nameof(Fixture.SpannerMock.CreateSession), ExecutionTime.FromMillis(20, 0));
        var builder = new SpannerConnectionStringBuilder
        {
            Host = Fixture.Host,
            Port = (uint) Fixture.Port,
            UsePlainText = true,
            DataSource = "projects/project1/instances/instance1/databases/database1",
        };
        await using var connection = new SpannerConnection();
        connection.ConnectionString = builder.ConnectionString;
        var tokenSource = new CancellationTokenSource(5);
        // TODO: Implement actual async opening of connections
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await connection.OpenAsync(tokenSource.Token));
        Assert.That(connection.State, Is.EqualTo(ConnectionState.Closed));
    }
    
    [Test]
    public void DataSourceProperty()
    {
        using var conn = new SpannerConnection();
        Assert.That(conn.DataSource, Is.EqualTo(string.Empty));

        var builder = new SpannerConnectionStringBuilder(ConnectionString);

        conn.ConnectionString = builder.ConnectionString;
        Assert.That(conn.DataSource, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
    }
    
    [Test]
    public void SettingConnectionStringWhileOpenThrows()
    {
        using var conn = new SpannerConnection();
        conn.ConnectionString = ConnectionString;
        conn.Open();
        Assert.That(() => conn.ConnectionString = "", Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void EmptyConstructor()
    {
        var conn = new SpannerConnection();
        Assert.That(conn.ConnectionTimeout, Is.EqualTo(15));
        Assert.That(conn.ConnectionString, Is.SameAs(string.Empty));
        Assert.That(() => conn.Open(), Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void ConstructorWithNullConnectionString()
    {
        var conn = new SpannerConnection((string?) null);
        Assert.That(conn.ConnectionString, Is.SameAs(string.Empty));
        Assert.That(() => conn.Open(), Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void ConstructorWithEmptyConnectionString()
    {
        var conn = new SpannerConnection("");
        Assert.That(conn.ConnectionString, Is.SameAs(string.Empty));
        Assert.That(() => conn.Open(), Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void SetConnectionStringToNull()
    {
        var conn = new SpannerConnection(ConnectionString);
        conn.ConnectionString = null;
        Assert.That(conn.ConnectionString, Is.SameAs(string.Empty));
        Assert.That(() => conn.Open(), Throws.Exception.TypeOf<InvalidOperationException>());
    }

    [Test]
    public void SetConnectionStringToEmpty()
    {
        var conn = new SpannerConnection(ConnectionString);
        conn.ConnectionString = "";
        Assert.That(conn.ConnectionString, Is.SameAs(string.Empty));
        Assert.That(() => conn.Open(), Throws.Exception.TypeOf<InvalidOperationException>());
    }
    
    [Test]
    public async Task ChangeDatabase()
    {
        await using var conn = await OpenConnectionAsync();
        Assert.That(conn.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
        conn.ChangeDatabase("template1");
        Assert.That(conn.Database, Is.EqualTo("projects/p1/instances/i1/databases/template1"));
    }

    [Test]
    public async Task ChangeDatabaseDoesNotAffectOtherConnections()
    {
        await using var conn1 = new SpannerConnection(ConnectionString);
        await using var conn2 = new SpannerConnection(ConnectionString);
        conn1.Open();
        conn1.ChangeDatabase("template1");
        Assert.That(conn1.Database, Is.EqualTo("projects/p1/instances/i1/databases/template1"));

        // Connection 2's database should not changed
        conn2.Open();
        Assert.That(conn2.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
    }
    
    [Test]
    public void ChangeDatabaseOnClosedConnectionWorks()
    {
        using var conn = new SpannerConnection(ConnectionString);
        Assert.That(conn.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
        conn.ChangeDatabase("template1");
        Assert.That(conn.Database, Is.EqualTo("projects/p1/instances/i1/databases/template1"));
    }

    [Test]
    public async Task ChangeDatabase_WithSharedConnectionStringBuilder_DoesNotAffectBuilderOrOtherConnections()
    {
        var builder = new SpannerConnectionStringBuilder(ConnectionString);
        await using var firstConnection = new SpannerConnection(builder);
        await using var secondConnection = new SpannerConnection(builder);

        await firstConnection.OpenAsync();
        firstConnection.ChangeDatabase("tenant_b");

        Assert.That(firstConnection.Database, Is.EqualTo("projects/p1/instances/i1/databases/tenant_b"));
        Assert.That(firstConnection.ConnectionString, Does.Contain("projects/p1/instances/i1/databases/tenant_b"));
        Assert.That(builder.DataSource, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
        Assert.That(secondConnection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
        Assert.That(secondConnection.ConnectionString, Does.Contain("projects/p1/instances/i1/databases/d1"));

        builder.DataSource = "projects/p1/instances/i1/databases/mutated_external";
        Assert.That(firstConnection.Database, Is.EqualTo("projects/p1/instances/i1/databases/tenant_b"));
        Assert.That(secondConnection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));

        await secondConnection.OpenAsync();
        Assert.That(await firstConnection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        Assert.That(await secondConnection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        var executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(2));
        Assert.That(executeRequests[0].Session, Does.StartWith("projects/p1/instances/i1/databases/tenant_b/sessions/"));
        Assert.That(executeRequests[1].Session, Does.StartWith("projects/p1/instances/i1/databases/d1/sessions/"));
    }

    [Test]
    public async Task ChangeDatabase_WithProjectInstanceDatabase_UpdatesDatabaseAndConnectionString()
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
        await using var connection = new SpannerConnection(builder);
        await connection.OpenAsync();
        Assert.That(connection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));

        connection.ChangeDatabase("d2");
        Assert.That(connection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d2"));
        Assert.That(connection.ConnectionString, Does.Contain("Database=d2"));
        Assert.That(builder.Database, Is.EqualTo("d1"));

        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        var executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(1));
        Assert.That(executeRequests[0].Session, Does.StartWith("projects/p1/instances/i1/databases/d2/sessions/"));

        connection.ChangeDatabase("projects/p2/instances/i2/databases/d3");
        Assert.That(connection.Database, Is.EqualTo("projects/p2/instances/i2/databases/d3"));
        Assert.That(connection.ConnectionString, Does.Contain("projects/p2/instances/i2/databases/d3"));
        Assert.That(connection.ConnectionString, Does.Not.Contain("Project="));
        Assert.That(connection.ConnectionString, Does.Not.Contain("Instance="));
        Assert.That(connection.ConnectionString, Does.Not.Contain("Database="));

        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(2));
        Assert.That(executeRequests[1].Session, Does.StartWith("projects/p2/instances/i2/databases/d3/sessions/"));
    }

    [Test]
    public void ChangeDatabase_WithInvalidDatabaseName_ThrowsAndPreservesOriginalState()
    {
        using var connection = new SpannerConnection(ConnectionString);
        var originalConnectionString = connection.ConnectionString;

        Assert.That(() => connection.ChangeDatabase("d2;UsePlainText=false"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("d2?usePlainText=false"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("d2\n"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("D2"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("projects/p1/instances/i1/databases/d2?type=OMNI"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("projects/p1/instances/i1/databases/d2;Host=evil.example"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("projects/p1/instances/i1/databases/d2\n"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("projects/p1;Host=evil/instances/i1/databases/d2"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("projects/p1/instances/i1?usePlainText=false/databases/d2"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("projects/P1/instances/i1/databases/d2"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("projects/p1/instances/I1/databases/d2"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("d2/extra"), Throws.ArgumentException);
        Assert.That(() => connection.ChangeDatabase("d2=foo"), Throws.ArgumentException);
        Assert.That(connection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d1"));
        Assert.That(connection.ConnectionString, Is.EqualTo(originalConnectionString));

        Assert.Throws<ArgumentException>(() => _ = new SpannerConnection("Data Source=not_a_resource_name"));

        using var omniDataSourceConnection = new SpannerConnection("Data Source=databases/d1");
        Assert.That(() => omniDataSourceConnection.ChangeDatabase("d2"), Throws.InvalidOperationException);
        Assert.That(omniDataSourceConnection.DataSource, Is.EqualTo("databases/d1"));

        using var mixedOmniDataSourceConnection = new SpannerConnection("Data Source=databases/d1;Project=p1;Instance=i1;Database=d1");
        Assert.That(() => mixedOmniDataSourceConnection.ChangeDatabase("d2"), Throws.InvalidOperationException);
        Assert.That(mixedOmniDataSourceConnection.Database, Is.EqualTo("databases/d1"));

        using var uninitializedConnection = new SpannerConnection();
        Assert.That(() => uninitializedConnection.ChangeDatabase("postgresql://evil.example/projects/p1/instances/i1/databases/d2?sslmode=disable"), Throws.ArgumentException);
        Assert.That(() => uninitializedConnection.ChangeDatabase("evil.example:9010/projects/p1/instances/i1/databases/d2"), Throws.ArgumentException);
        Assert.That(() => uninitializedConnection.ChangeDatabase("d2"), Throws.InvalidOperationException);
        Assert.That(uninitializedConnection.ConnectionString, Is.EqualTo(string.Empty));
    }

    [Test]
    public void ChangeDatabase_OnUninitializedConnectionWithFullResourceName_SetsDataSource()
    {
        using var connection = new SpannerConnection();
        connection.ChangeDatabase("projects/p1/instances/i1/databases/d2");
        Assert.That(connection.Database, Is.EqualTo("projects/p1/instances/i1/databases/d2"));
        Assert.That(connection.DataSource, Is.EqualTo("projects/p1/instances/i1/databases/d2"));
        Assert.That(connection.ConnectionString, Is.EqualTo("Data Source=projects/p1/instances/i1/databases/d2"));

        connection.ChangeDatabase("projects/DEFAULT_PROJECT_ID/instances/i1/databases/d3");
        Assert.That(connection.Database, Is.EqualTo("projects/DEFAULT_PROJECT_ID/instances/i1/databases/d3"));

        connection.ChangeDatabase("projects/example.com:my-project/instances/i1/databases/d4");
        Assert.That(connection.Database, Is.EqualTo("projects/example.com:my-project/instances/i1/databases/d4"));
    }

    [Test]
    [Ignore("Must add search_path connection property in shared library first")]
    public async Task SearchPath()
    {
        // TODO: Add search_path connection variable in shared library
        await using var dataSource = CreateDataSource(csb => csb.SearchPath = "foo");
        await using var conn = await dataSource.OpenConnectionAsync() as SpannerConnection;
        Assert.That(await conn!.ExecuteScalarAsync("SHOW VARIABLE search_path"), Contains.Substring("foo"));
    }

    [Test]
    public async Task SetOptions()
    {
        await using var dataSource = CreateDataSource(csb => csb.Options = "isolation_level=serializable;read_lock_mode=pessimistic");
        await using var conn = await dataSource.OpenConnectionAsync() as SpannerConnection;

        Assert.That(await conn!.ExecuteScalarAsync("SHOW VARIABLE isolation_level"), Is.EqualTo("Serializable"));
        Assert.That(await conn!.ExecuteScalarAsync("SHOW VARIABLE read_lock_mode"), Is.EqualTo("PESSIMISTIC"));
    }
    
    [Test]
    public async Task ConnectorNotInitializedException()
    {
        var command = new SpannerCommand();
        command.CommandText = "SELECT 1";

        for (var i = 0; i < 2; i++)
        {
            await using var connection = await OpenConnectionAsync();
            command.Connection = connection;
            await using var tx = await connection.BeginTransactionAsync();
            await command.ExecuteScalarAsync();
            await tx.CommitAsync();
        }
    }
    
    [Test]
    public void ConnectionStateIsClosedWhenDisposed()
    {
        var c = new SpannerConnection();
        c.Dispose();
        Assert.That(c.State, Is.EqualTo(ConnectionState.Closed));
    }
    
    [Test]
    public async Task ConcurrentReadersAllowed()
    {
        await using var conn = await OpenConnectionAsync();
        await using var cmd = new SpannerCommand("SELECT 1", conn);
        await using (await cmd.ExecuteReaderAsync())
        {
            Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        }
    }
    
    [Test]
    public async Task ManyOpenClose()
    {
        await using var dataSource = CreateDataSource();
        for (var i = 0; i < 256; i++)
        {
            await using var conn = await dataSource.OpenConnectionAsync();
        }
        await using (var conn = dataSource.CreateConnection())
        {
            await conn.OpenAsync();
        }
        await using (var conn = dataSource.CreateConnection() as SpannerConnection)
        {
            await conn!.OpenAsync();
            Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        }
    }

    [Test]
    public async Task ManyOpenCloseWithTransaction()
    {
        await using var dataSource = CreateDataSource();
        for (var i = 0; i < 256; i++)
        {
            await using var conn = await dataSource.OpenConnectionAsync();
            await conn.BeginTransactionAsync();
        }

        await using (var conn = await dataSource.OpenConnectionAsync() as SpannerConnection)
        {
            Assert.That(await conn!.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        }
    }
    
    [Test]
    public async Task RollbackOnClose()
    {
        await using var dataSource = CreateDataSource();
        await using (var conn = await dataSource.OpenConnectionAsync())
        {
            await conn!.BeginTransactionAsync();
            await conn.ExecuteNonQueryAsync("SELECT 1");
            Assert.That(conn.HasTransaction);
        }
        await using (var conn = await dataSource.OpenConnectionAsync())
        {
            Assert.That(conn.HasTransaction, Is.False);
        }
    }

    [Test]
    public async Task ReadLargeString()
    {
        const string sql = "select large_value from my_table";
        var value = TestUtils.GenerateRandomString(10_000_000);
        Fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(
            new V1.Type{Code = TypeCode.String}, "large_value", value));
        
        await using var dataSource = CreateDataSource();
        await using var conn = await dataSource.OpenConnectionAsync() as SpannerConnection;
        var got = await conn!.ExecuteScalarAsync(sql);
        Assert.That(got, Is.EqualTo(value));
    }

    private class TestLibObject : Google.Cloud.SpannerLib.AbstractLibObject
    {
        public bool CloseLibObjectCalled { get; private set; }

        public TestLibObject(long id) : base(null!, id)
        {
        }

        protected override void CloseLibObject()
        {
            CloseLibObjectCalled = true;
        }

        public void CallDispose(bool disposing)
        {
            Dispose(disposing);
        }
    }

    [Test]
    public void Finalizer_DoesNotCallCloseLibObject()
    {
        var testObj = new TestLibObject(id: 42);

        testObj.CallDispose(disposing: false);

        Assert.That(testObj.CloseLibObjectCalled, Is.False);
    }

    [Test]
    public void Dispose_CallsCloseLibObject()
    {
        var testObj = new TestLibObject(id: 42);

        testObj.CallDispose(disposing: true);

        Assert.That(testObj.CloseLibObjectCalled, Is.True);
    }

    [Test]
    public async Task Open_WithValidatedProjectInstanceDatabaseAndOptions_Succeeds()
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder
        {
            Host = $"{Fixture.Host}:{Fixture.Port}",
            Project = "p1",
            Instance = "i1",
            Database = "d1",
            UsePlainText = true,
            Options = "isolation_level=serializable",
        };

        await using var connection = new SpannerConnection(connectionStringBuilder.ConnectionString);
        await connection.OpenAsync();
        Assert.That(await connection.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));

        var executeRequests = Fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
        Assert.That(executeRequests, Has.Count.EqualTo(1));
        Assert.That(executeRequests[0].Session, Does.StartWith("projects/p1/instances/i1/databases/d1/sessions/"));
    }

    [Test]
    public void Constructor_WithInjectedConnectionStringValues_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => _ = new SpannerConnection("Data Source=\"projects/p1/instances/i1/databases/d1;usePlainText=true\""));
        Assert.Throws<ArgumentException>(() => _ = new SpannerConnection("Host=\"evil.com/projects/p1/instances/i1/databases/d1;usePlainText=true;x=\";Data Source=projects/p1/instances/i1/databases/d1"));
        Assert.Throws<ArgumentException>(() => _ = new SpannerConnection("Data Source=projects/p1/instances/i1/databases/d1;UnknownOption=\"val;usePlainText=true\""));
        Assert.Throws<ArgumentException>(() => _ = SpannerDataSource.Create("Data Source=\"projects/p1/instances/i1/databases/d1;usePlainText=true\""));
    }
}