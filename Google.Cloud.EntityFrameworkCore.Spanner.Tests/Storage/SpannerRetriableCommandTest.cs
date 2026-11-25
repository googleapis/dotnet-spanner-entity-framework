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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.Admin.Database.V1;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Storage;

#pragma warning disable EF1001 // Internal EF Core API usage.
public class SpannerRetriableCommandTest : IClassFixture<SpannerMockServerFixture>
{
    private const string Database1Prefix = "projects/p1/instances/i1/databases/d1";
    private const string Database2Prefix = "projects/p1/instances/i1/databases/d2";
    private const string SelectSql = "SELECT 1";
    private const string UpdateSql = "UPDATE Foo SET Bar='bar' WHERE Id=1";
    private const string DdlSql = "CREATE TABLE Foo (Id INT64 NOT NULL) PRIMARY KEY (Id)";

    private readonly SpannerMockServerFixture _fixture;

    public SpannerRetriableCommandTest(SpannerMockServerFixture fixture)
    {
        _fixture = fixture;
        _fixture.SpannerMock.Reset();
        _fixture.DatabaseAdminMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            SelectSql,
            StatementResult.CreateResultSet(
                new List<Tuple<TypeCode, string>> { Tuple.Create(TypeCode.Int64, "Col") },
                new List<object[]> { new object[] { 1L } }));
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            UpdateSql,
            StatementResult.CreateUpdateCount(1));
    }

    private SpannerRetriableConnection CreateConnection(string databaseName = "d1")
    {
        var connectionString = $"Data Source=projects/p1/instances/i1/databases/{databaseName};Host={_fixture.Host};Port={_fixture.Port}";
        var builder = new SpannerConnectionStringBuilder(connectionString, ChannelCredentials.Insecure);
        return new SpannerRetriableConnection(new SpannerConnection(builder));
    }

    private IReadOnlyList<ExecuteSqlRequest> GetExecuteSqlRequests() =>
        _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();

    private IReadOnlyList<CommitRequest> GetCommitRequests() =>
        _fixture.SpannerMock.Requests.OfType<CommitRequest>().ToList();

    private IReadOnlyList<UpdateDatabaseDdlRequest> GetDdlRequests() =>
        _fixture.DatabaseAdminMock.Requests.OfType<UpdateDatabaseDdlRequest>().ToList();

    [Fact]
    public async Task ExecuteNonQuery_SyncAndAsync_ParityWithoutTransaction()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var syncCommand = connection.CreateDmlCommand(UpdateSql);
        var syncResult = syncCommand.ExecuteNonQuery();
        Assert.Equal(1, syncResult);

        using var asyncCommand = connection.CreateDmlCommand(UpdateSql);
        var asyncResult = await asyncCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, asyncResult);
    }

    [Fact]
    public async Task ExecuteDbDataReader_SyncAndAsync_ParityWithTimestampBound()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var syncCommand = connection.CreateSelectCommand(SelectSql);
        syncCommand.TimestampBound = TimestampBound.Strong;
        using var syncReader = syncCommand.ExecuteReader();
        Assert.True(syncReader.Read());
        Assert.Equal(1L, syncReader.GetInt64(0));

        using var asyncCommand = connection.CreateSelectCommand(SelectSql);
        asyncCommand.TimestampBound = TimestampBound.Strong;
        using var asyncReader = await asyncCommand.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await asyncReader.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1L, asyncReader.GetInt64(0));
    }

    [Fact]
    public void ExecuteScalar_ReusedCommandWithTimestampBoundReset_UpdatesEphemeralTransactionCreationOptions()
    {
        using var connection = CreateConnection();
        connection.Open();

        using var command = connection.CreateSelectCommand(SelectSql);
        var initialResult = command.ExecuteScalar();
        Assert.Equal(1L, initialResult);
        Assert.Null(command.SpannerCommand.EphemeralTransactionCreationOptions);

        command.TimestampBound = TimestampBound.Strong;
        var boundResult = command.ExecuteScalar();
        Assert.Equal(1L, boundResult);
        Assert.NotNull(command.SpannerCommand.EphemeralTransactionCreationOptions);

        command.TimestampBound = null;
        var resetResult = command.ExecuteScalar();
        Assert.Equal(1L, resetResult);
        Assert.Null(command.SpannerCommand.EphemeralTransactionCreationOptions);
    }

    [Fact]
    public void ExecuteDbDataReader_ReusedCommandWithTimestampBoundReset_UpdatesEphemeralTransactionCreationOptions()
    {
        using var connection = CreateConnection();
        connection.Open();

        using var command = connection.CreateSelectCommand(SelectSql);
        using (var initialReader = command.ExecuteReader())
        {
            Assert.True(initialReader.Read());
            Assert.Equal(1L, initialReader.GetInt64(0));
        }
        Assert.Null(command.SpannerCommand.EphemeralTransactionCreationOptions);

        command.TimestampBound = TimestampBound.Strong;
        using (var boundReader = command.ExecuteReader())
        {
            Assert.True(boundReader.Read());
            Assert.Equal(1L, boundReader.GetInt64(0));
        }
        Assert.NotNull(command.SpannerCommand.EphemeralTransactionCreationOptions);

        command.TimestampBound = null;
        using (var resetReader = command.ExecuteReader())
        {
            Assert.True(resetReader.Read());
            Assert.Equal(1L, resetReader.GetInt64(0));
        }
        Assert.Null(command.SpannerCommand.EphemeralTransactionCreationOptions);
    }

    [Fact]
    public async Task ExecuteDbDataReaderAsync_ReusedCommandWithTimestampBoundReset_UpdatesEphemeralTransactionCreationOptions()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection.CreateSelectCommand(SelectSql);
        using (var initialReader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            Assert.True(await initialReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, initialReader.GetInt64(0));
        }
        Assert.Null(command.SpannerCommand.EphemeralTransactionCreationOptions);

        command.TimestampBound = TimestampBound.Strong;
        using (var boundReader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            Assert.True(await boundReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, boundReader.GetInt64(0));
        }
        Assert.NotNull(command.SpannerCommand.EphemeralTransactionCreationOptions);

        command.TimestampBound = null;
        using (var resetReader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            Assert.True(await resetReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, resetReader.GetInt64(0));
        }
        Assert.Null(command.SpannerCommand.EphemeralTransactionCreationOptions);
    }

    [Fact]
    public void ExecuteNonQuery_CommandCanBeReusedAfterExecution()
    {
        using var connection = CreateConnection();
        connection.Open();

        using var command = connection.CreateDmlCommand(UpdateSql);
        var affectedRows = command.ExecuteNonQuery();
        Assert.Equal(1, affectedRows);
        Assert.Null(command.Transaction);

        // Reusing the command should not throw InvalidOperationException from a disposed transaction
        command.CommandText = SelectSql;
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_CommandCanBeReusedAfterExecution()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection.CreateDmlCommand(UpdateSql);
        var affectedRows = await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, affectedRows);
        Assert.Null(command.Transaction);

        // Reusing the command after async execution should not throw ObjectDisposedException from a disposed transaction
        command.CommandText = SelectSql;
        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExecuteScalarAsync_ReusedCommandWithTimestampBoundReset_UpdatesEphemeralTransactionCreationOptions()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection.CreateSelectCommand(SelectSql);
        var initialResult = await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1L, initialResult);
        Assert.Null(command.SpannerCommand.EphemeralTransactionCreationOptions);

        command.TimestampBound = TimestampBound.Strong;
        var boundResult = await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1L, boundResult);
        Assert.NotNull(command.SpannerCommand.EphemeralTransactionCreationOptions);

        command.TimestampBound = null;
        var resetResult = await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1L, resetResult);
        Assert.Null(command.SpannerCommand.EphemeralTransactionCreationOptions);
    }

    [Fact]
    public void ExecuteNonQuery_StandaloneDml_RetriesOnAbortedAndResetsTransaction()
    {
        using var connection = CreateConnection();
        connection.Open();

        using var command = connection.CreateDmlCommand(UpdateSql);
        _fixture.SpannerMock.AbortNextStatement();

        var affectedRows = command.ExecuteNonQuery();
        Assert.Equal(1, affectedRows);
        Assert.Null(command.Transaction);

        command.CommandText = SelectSql;
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_StandaloneDml_RetriesOnAbortedAndResetsTransaction()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection.CreateDmlCommand(UpdateSql);
        _fixture.SpannerMock.AbortNextStatement();

        var affectedRows = await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, affectedRows);
        Assert.Null(command.Transaction);

        command.CommandText = SelectSql;
        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DbConnectionSetter_ReassignedToDifferentConnection_RoutesQueriesToNewConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection1.CreateSelectCommand(SelectSql);
        Assert.Equal(1L, command.ExecuteScalar());

        command.Connection = connection2;
        Assert.Same(connection2, command.Connection);

        Assert.Equal(1L, command.ExecuteScalar());
        Assert.Equal(1L, await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        using (var reader = command.ExecuteReader())
        {
            Assert.True(reader.Read());
            Assert.Equal(1L, reader.GetInt64(0));
        }

        await using (var asyncReader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            Assert.True(await asyncReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, asyncReader.GetInt64(0));
        }

        command.TimestampBound = TimestampBound.Strong;
        using (var timestampBoundReader = command.ExecuteReader())
        {
            Assert.True(timestampBoundReader.Read());
            Assert.Equal(1L, timestampBoundReader.GetInt64(0));
        }

        await using (var asyncTimestampBoundReader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            Assert.True(await asyncTimestampBoundReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, asyncTimestampBoundReader.GetInt64(0));
        }

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(7, sqlRequests.Count);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[0].Session);
        for (var index = 1; index < sqlRequests.Count; index++)
        {
            Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[index].Session);
        }
    }

    [Fact]
    public async Task CreateCommand_ReassignedToDifferentConnection_RoutesQueriesToNewConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection1.CreateCommand();
        command.CommandText = SelectSql;
        command.Connection = connection2;

        Assert.Equal(1L, await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        var sqlRequest = Assert.Single(GetExecuteSqlRequests());
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequest.Session);
    }

    [Fact]
    public async Task DbConnectionSetter_ReassignedToDifferentConnection_RoutesDmlAndSubsequentQueryToNewConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection1.CreateDmlCommand(UpdateSql);
        Assert.Equal(1, command.ExecuteNonQuery());

        command.Connection = connection2;
        Assert.Equal(1, command.ExecuteNonQuery());
        Assert.Equal(1, await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));

        command.CommandText = SelectSql;
        Assert.Equal(1L, command.ExecuteScalar());

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(4, sqlRequests.Count);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[0].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[1].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[2].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[3].Session);
    }

    [Fact]
    public async Task DbConnectionSetter_ReassignedToDifferentConnection_RoutesDdlToNewConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection1.CreateDdlCommand(DdlSql);
        command.ExecuteNonQuery();

        command.Connection = connection2;
        command.ExecuteNonQuery();
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        await command.StartDdlOperationAsync(TestContext.Current.CancellationToken);

        var ddlRequests = GetDdlRequests();
        Assert.Equal(4, ddlRequests.Count);
        Assert.Equal(Database1Prefix, ddlRequests[0].Database);
        Assert.Equal(Database2Prefix, ddlRequests[1].Database);
        Assert.Equal(Database2Prefix, ddlRequests[2].Database);
        Assert.Equal(Database2Prefix, ddlRequests[3].Database);
    }

    [Fact]
    public async Task DbTransactionSetter_ClearedBeforeReassigningConnection_DoesNotUsePreviousTransaction()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var transaction1 = await connection1.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        command.Transaction = transaction1;
        Assert.Equal(1L, command.ExecuteScalar());

        command.Transaction = null;
        command.Connection = connection2;

        Assert.Equal(1L, command.ExecuteScalar());
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1L, reader.GetInt64(0));

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(3, sqlRequests.Count);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[0].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[1].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[2].Session);
    }

    [Fact]
    public async Task DbTransactionSetter_ReadOnlyTransactionClearedBeforeReassigningConnection_DoesNotUsePreviousTransaction()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var readOnlyTransaction1 = await connection1.BeginReadOnlyTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        command.Transaction = readOnlyTransaction1;
        Assert.Equal(1L, command.ExecuteScalar());

        command.Transaction = null;
        command.Connection = connection2;
        Assert.Equal(1L, await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(2, sqlRequests.Count);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[0].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[1].Session);
    }

    private static async Task AssertAllExecutionMethodsThrowInvalidOperationAsync(SpannerRetriableCommand command)
    {
        Assert.Throws<InvalidOperationException>(() => command.ExecuteScalar());
        await Assert.ThrowsAsync<InvalidOperationException>(() => command.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());
        await Assert.ThrowsAsync<InvalidOperationException>(() => command.ExecuteReaderAsync(TestContext.Current.CancellationToken));
        Assert.Throws<InvalidOperationException>(() => command.ExecuteNonQuery());
        await Assert.ThrowsAsync<InvalidOperationException>(() => command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<InvalidOperationException>(() => command.StartDdlOperationAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Execute_MismatchedConnectionAndTransaction_ThrowsInvalidOperationException()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var transaction1 = await connection1.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        command.Transaction = transaction1;
        command.Connection = connection2;

        await AssertAllExecutionMethodsThrowInvalidOperationAsync(command);

        command.TimestampBound = TimestampBound.Strong;
        await AssertAllExecutionMethodsThrowInvalidOperationAsync(command);

        command.CommandText = DdlSql;
        await AssertAllExecutionMethodsThrowInvalidOperationAsync(command);

        Assert.Empty(GetExecuteSqlRequests());
        Assert.Empty(GetDdlRequests());
    }

    [Fact]
    public async Task Execute_MismatchedTransactionAssignedToCommand_ThrowsInvalidOperationException()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var transaction2 = await connection2.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var readOnlyTransaction2 = await connection2.BeginReadOnlyTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);

        command.Transaction = transaction2;
        await AssertAllExecutionMethodsThrowInvalidOperationAsync(command);

        command.Transaction = readOnlyTransaction2;
        await AssertAllExecutionMethodsThrowInvalidOperationAsync(command);

        Assert.Empty(GetExecuteSqlRequests());
        Assert.Empty(GetDdlRequests());
    }

    [Fact]
    public async Task Execute_MismatchedConnectionAndTransaction_RecoversAfterFixingTransaction()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var transaction1 = await connection1.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var transaction2 = await connection2.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        command.Transaction = transaction1;
        command.Connection = connection2;

        await AssertAllExecutionMethodsThrowInvalidOperationAsync(command);
        Assert.Empty(GetExecuteSqlRequests());

        command.Transaction = transaction2;
        Assert.Equal(1L, command.ExecuteScalar());

        command.Transaction = null;
        Assert.Equal(1L, await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        await transaction2.CommitAsync(TestContext.Current.CancellationToken);

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(2, sqlRequests.Count);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[0].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[1].Session);
    }

    [Fact]
    public async Task Execute_TransactionAssignedBeforeMatchingConnection_SucceedsOnNewConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var transaction2 = await connection2.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        command.Transaction = transaction2;
        command.Connection = connection2;

        Assert.Equal(1L, command.ExecuteScalar());
        await transaction2.CommitAsync(TestContext.Current.CancellationToken);

        var sqlRequest = Assert.Single(GetExecuteSqlRequests());
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequest.Session);
    }

    [Fact]
    public async Task DbConnectionSetter_SetToNullThenReassigned_ThrowsWhileNullAndSucceedsAfterReassign()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection1.CreateDmlCommand(UpdateSql);
        command.Connection = null;

        Assert.Null(command.Connection);
        await AssertAllExecutionMethodsThrowInvalidOperationAsync(command);

        command.CommandText = DdlSql;
        await AssertAllExecutionMethodsThrowInvalidOperationAsync(command);

        Assert.Empty(GetExecuteSqlRequests());
        Assert.Empty(GetDdlRequests());

        command.CommandText = UpdateSql;
        command.Connection = connection2;
        Assert.Equal(1, command.ExecuteNonQuery());

        var sqlRequest = Assert.Single(GetExecuteSqlRequests());
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequest.Session);
    }

    [Fact]
    public async Task Execute_NullConnectionWithActiveTransaction_ThrowsInvalidOperationException()
    {
        using var connection1 = CreateConnection("d1");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);

        using var transaction1 = await connection1.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        command.Transaction = transaction1;
        command.Connection = null;

        await AssertAllExecutionMethodsThrowInvalidOperationAsync(command);
        Assert.Empty(GetExecuteSqlRequests());
        Assert.Empty(GetDdlRequests());
    }

    [Fact]
    public async Task DbConnectionSetter_UnsupportedConnectionType_ThrowsInvalidCastExceptionWithoutMutatingConnection()
    {
        using var connection1 = CreateConnection("d1");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        using var rawSpannerConnection = new SpannerConnection($"Data Source={Database2Prefix};Host={_fixture.Host};Port={_fixture.Port}", ChannelCredentials.Insecure);

        Assert.Throws<InvalidCastException>(() => command.Connection = rawSpannerConnection);
        Assert.Same(connection1, command.Connection);
        Assert.Equal(1L, command.ExecuteScalar());

        var sqlRequest = Assert.Single(GetExecuteSqlRequests());
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequest.Session);
    }

    [Fact]
    public void Constructor_NullSpannerCommand_ThrowsArgumentNullException()
    {
        using var connection1 = CreateConnection("d1");

        Assert.Throws<ArgumentNullException>(() => new SpannerRetriableCommand(connection1, null));
    }

    [Fact]
    public async Task Constructor_NullConnectionWithBoundSpannerCommand_ClearsWrappedConnectionAndThrowsUntilAssigned()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var innerCommand = connection1.SpannerConnection.CreateSelectCommand(SelectSql);
        using var command = new SpannerRetriableCommand(null, innerCommand);

        Assert.Null(command.Connection);
        await AssertAllExecutionMethodsThrowInvalidOperationAsync(command);
        Assert.Empty(GetExecuteSqlRequests());

        command.Connection = connection2;
        Assert.Equal(1L, command.ExecuteScalar());

        var sqlRequest = Assert.Single(GetExecuteSqlRequests());
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequest.Session);
    }

    [Fact]
    public async Task Constructor_MismatchedInnerSpannerCommandConnection_NormalizesToRetriableConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var innerCommand = connection1.SpannerConnection.CreateSelectCommand(SelectSql);
        using var command = new SpannerRetriableCommand(connection2, innerCommand);

        Assert.Equal(1L, command.ExecuteScalar());

        var sqlRequest = Assert.Single(GetExecuteSqlRequests());
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequest.Session);
    }

    [Fact]
    public async Task DbConnectionSetter_RoundTripBetweenConnections_RoutesEachExecutionToCurrentConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection1.CreateSelectCommand(SelectSql);
        Assert.Equal(1L, command.ExecuteScalar());

        command.Connection = connection2;
        Assert.Equal(1L, command.ExecuteScalar());

        command.Connection = connection1;
        Assert.Equal(1L, await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(3, sqlRequests.Count);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[0].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[1].Session);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[2].Session);
    }

    [Fact]
    public async Task DbConnectionSetter_SameConnectionReassignedWithActiveTransaction_PreservesTransaction()
    {
        using var connection1 = CreateConnection("d1");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);

        using var transaction1 = await connection1.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        command.Transaction = transaction1;
        command.Connection = connection1;

        Assert.Same(transaction1, command.Transaction);
        Assert.Equal(1L, command.ExecuteScalar());
        await transaction1.CommitAsync(TestContext.Current.CancellationToken);

        var sqlRequest = Assert.Single(GetExecuteSqlRequests());
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequest.Session);
    }

    [Fact]
    public async Task DbConnectionSetter_ReassignedWithDmlAbortRetry_RetriesOnNewConnectionAndClearsEphemeralTransaction()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var asyncCommand = connection1.CreateDmlCommand(UpdateSql);
        Assert.Equal(1, await asyncCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        asyncCommand.Connection = connection2;

        _fixture.SpannerMock.AbortNextStatement();
        Assert.Equal(1, await asyncCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));

        asyncCommand.CommandText = SelectSql;
        await using (var asyncReader = await asyncCommand.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            Assert.True(await asyncReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, asyncReader.GetInt64(0));
        }

        using var syncCommand = connection1.CreateDmlCommand(UpdateSql);
        Assert.Equal(1, syncCommand.ExecuteNonQuery());
        syncCommand.Connection = connection2;

        _fixture.SpannerMock.AbortNextStatement();
        Assert.Equal(1, syncCommand.ExecuteNonQuery());

        syncCommand.CommandText = SelectSql;
        Assert.Equal(1L, syncCommand.ExecuteScalar());

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(8, sqlRequests.Count);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[0].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[1].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[2].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[3].Session);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[4].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[5].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[6].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[7].Session);
    }

    [Fact]
    public async Task DbConnectionSetter_ReassignedAfterNonAbortedDmlFailure_ClearsEphemeralTransaction()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        const string invalidUpdateSql = "UPDATE MissingTable SET Bar='bar' WHERE Id=1";
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            invalidUpdateSql,
            StatementResult.CreateException(new RpcException(new Status(StatusCode.InvalidArgument, "Invalid statement"))));

        using var syncCommand = connection1.CreateDmlCommand(invalidUpdateSql);
        Assert.Throws<SpannerException>(() => syncCommand.ExecuteNonQuery());
        syncCommand.Connection = connection2;
        syncCommand.CommandText = SelectSql;
        Assert.Equal(1L, syncCommand.ExecuteScalar());

        using var asyncCommand = connection1.CreateDmlCommand(invalidUpdateSql);
        await Assert.ThrowsAsync<SpannerException>(() => asyncCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        asyncCommand.Connection = connection2;
        asyncCommand.CommandText = SelectSql;
        Assert.Equal(1L, await asyncCommand.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        var selectRequests = GetExecuteSqlRequests().Where(request => request.Sql == SelectSql).ToList();
        Assert.Equal(2, selectRequests.Count);
        Assert.StartsWith($"{Database2Prefix}/sessions/", selectRequests[0].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", selectRequests[1].Session);
    }

    [Fact]
    public async Task DbConnectionSetter_ChangedBeforeTransactionCleared_RoutesToNewConnectionAfterClearingTransaction()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var transaction1 = await connection1.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        command.Transaction = transaction1;
        Assert.Equal(1L, command.ExecuteScalar());

        command.Connection = connection2;
        command.Transaction = null;

        Assert.Equal(1L, command.ExecuteScalar());

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(2, sqlRequests.Count);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[0].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[1].Session);
    }

    [Fact]
    public async Task DbTransactionSetter_RetriableTransactionDmlClearedAndDisposed_ReassigningConnectionExecutesOnNewConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var initialCommand = connection1.CreateDmlCommand(UpdateSql);
        using (var transaction1 = await connection1.BeginTransactionAsync(TestContext.Current.CancellationToken))
        {
            initialCommand.Transaction = transaction1;
            Assert.Equal(1, initialCommand.ExecuteNonQuery());
            await transaction1.CommitAsync(TestContext.Current.CancellationToken);
        }

        initialCommand.Transaction = null;
        initialCommand.Connection = connection2;

        Assert.Equal(1, await initialCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        initialCommand.CommandText = SelectSql;
        Assert.Equal(1L, initialCommand.ExecuteScalar());

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(3, sqlRequests.Count);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[0].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[1].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[2].Session);
    }

    [Fact]
    public async Task DbTransactionSetter_ReassignedAcrossConnections_RoutesAllOperationsToTargetTransaction()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var transaction1 = await connection1.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var transaction2 = await connection2.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var readOnlyTransaction2 = await connection2.BeginReadOnlyTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateDmlCommand(UpdateSql);
        command.Transaction = transaction1;
        Assert.Equal(1, command.ExecuteNonQuery());

        command.Connection = connection2;
        command.Transaction = transaction2;
        Assert.Equal(1, await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));

        command.CommandText = SelectSql;
        Assert.Equal(1L, command.ExecuteScalar());
        Assert.Equal(1L, await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        using (var reader = command.ExecuteReader())
        {
            Assert.True(reader.Read());
            Assert.Equal(1L, reader.GetInt64(0));
        }

        await using (var asyncReader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            Assert.True(await asyncReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, asyncReader.GetInt64(0));
        }

        await transaction1.CommitAsync(TestContext.Current.CancellationToken);
        await transaction2.CommitAsync(TestContext.Current.CancellationToken);

        command.Transaction = readOnlyTransaction2;
        Assert.Equal(1L, command.ExecuteScalar());
        await using (var readOnlyAsyncReader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            Assert.True(await readOnlyAsyncReader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, readOnlyAsyncReader.GetInt64(0));
        }

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(8, sqlRequests.Count);
        Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequests[0].Session);
        for (var index = 1; index < sqlRequests.Count; index++)
        {
            Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[index].Session);
        }
    }

    [Fact]
    public async Task DbTransactionSetter_UnsupportedTransactionType_ThrowsInvalidCastExceptionWithoutMutatingTransaction()
    {
        using var connection1 = CreateConnection("d1");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        using var transaction1 = await connection1.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        using var rawTransaction = connection1.SpannerConnection.BeginTransaction();

        Assert.Throws<InvalidCastException>(() => command.Transaction = rawTransaction);
        Assert.Null(command.Transaction);

        command.Transaction = transaction1;
        Assert.Throws<InvalidCastException>(() => command.Transaction = rawTransaction);
        Assert.Same(transaction1, command.Transaction);
    }

    [Fact]
    public async Task Constructor_InnerSpannerCommandWithTransaction_ClearsClonedTransaction()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var rawTransaction1 = await connection1.SpannerConnection.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var innerCommand = connection1.SpannerConnection.CreateSelectCommand(SelectSql);
        innerCommand.Transaction = rawTransaction1;

        using var command = new SpannerRetriableCommand(connection2, innerCommand);
        Assert.Null(command.Transaction);
        Assert.Equal(1L, command.ExecuteScalar());
        Assert.Equal(1L, await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));

        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(2, sqlRequests.Count);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[0].Session);
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequests[1].Session);
    }

    [Fact]
    public async Task DbConnectionSetter_ReassignedAfterTransactionExecution_TransactionRetriesRemainOnOriginalConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var transaction1 = await connection1.BeginTransactionAsync(TestContext.Current.CancellationToken);
        using var command = connection1.CreateSelectCommand(SelectSql);
        command.Transaction = transaction1;

        await using (var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1L, reader.GetInt64(0));
        }

        command.CommandText = UpdateSql;
        Assert.Equal(1, await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));

        command.Transaction = null;
        command.Connection = connection2;

        _fixture.SpannerMock.AbortTransaction(transaction1.TransactionId);
        using var followUpCommand = connection1.CreateDmlCommand(UpdateSql);
        followUpCommand.Transaction = transaction1;
        Assert.Equal(1, await followUpCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, transaction1.RetryCount);

        _fixture.SpannerMock.AbortTransaction(transaction1.TransactionId);
        await transaction1.CommitAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, transaction1.RetryCount);
        var sqlRequests = GetExecuteSqlRequests();
        Assert.Equal(9, sqlRequests.Count);
        foreach (var sqlRequest in sqlRequests)
        {
            Assert.StartsWith($"{Database1Prefix}/sessions/", sqlRequest.Session);
        }

        var commitRequests = GetCommitRequests();
        Assert.Equal(2, commitRequests.Count);
        foreach (var commitRequest in commitRequests)
        {
            Assert.StartsWith($"{Database1Prefix}/sessions/", commitRequest.Session);
        }
    }

    [Fact]
    public async Task DbConnectionSetter_ReassignedToDifferentConnection_RoutesMutationsToNewConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var insertCommand = connection1.CreateInsertCommand("Foo");
        insertCommand.Parameters.Add(new SpannerParameter("Id", SpannerDbType.Int64, 1L));
        insertCommand.Connection = connection2;
        Assert.Equal(1, insertCommand.ExecuteNonQuery());

        using var updateCommand = connection1.CreateUpdateCommand("Foo");
        updateCommand.Parameters.Add(new SpannerParameter("Id", SpannerDbType.Int64, 1L));
        updateCommand.Connection = connection2;
        Assert.Equal(1, await updateCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));

        using var insertOrUpdateCommand = connection1.CreateInsertOrUpdateCommand("Foo");
        insertOrUpdateCommand.Parameters.Add(new SpannerParameter("Id", SpannerDbType.Int64, 1L));
        insertOrUpdateCommand.Connection = connection2;
        Assert.Equal(1, insertOrUpdateCommand.ExecuteNonQuery());

        using var deleteCommand = connection1.CreateDeleteCommand("Foo");
        deleteCommand.Parameters.Add(new SpannerParameter("Id", SpannerDbType.Int64, 1L));
        deleteCommand.Connection = connection2;
        Assert.Equal(1, await deleteCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));

        var commitRequests = GetCommitRequests();
        Assert.Equal(4, commitRequests.Count);
        foreach (var commitRequest in commitRequests)
        {
            Assert.StartsWith($"{Database2Prefix}/sessions/", commitRequest.Session);
        }
    }

    [Fact]
    public async Task Prepare_AfterReassigningConnection_DoesNotThrowAndExecutesOnNewConnection()
    {
        using var connection1 = CreateConnection("d1");
        using var connection2 = CreateConnection("d2");
        await connection1.OpenAsync(TestContext.Current.CancellationToken);
        await connection2.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection1.CreateSelectCommand(SelectSql);
        command.Connection = null;
        command.Prepare();

        command.Connection = connection2;
        command.Prepare();
        Assert.Equal(1L, command.ExecuteScalar());

        var sqlRequest = Assert.Single(GetExecuteSqlRequests());
        Assert.StartsWith($"{Database2Prefix}/sessions/", sqlRequest.Session);
    }
}
#pragma warning restore EF1001
