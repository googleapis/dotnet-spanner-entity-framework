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
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Storage;

#pragma warning disable EF1001 // Internal EF Core API usage.
public class SpannerRetriableCommandTest : IClassFixture<SpannerMockServerFixture>
{
    private readonly SpannerMockServerFixture _fixture;

    public SpannerRetriableCommandTest(SpannerMockServerFixture fixture)
    {
        _fixture = fixture;
        ResetMockServer();
    }

    private void ResetMockServer()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            "SELECT 1",
            StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>> { Tuple.Create(V1.TypeCode.Int64, "Col") },
                new List<object[]> { new object[] { 1L } }));
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            "UPDATE Foo SET Bar='bar' WHERE Id=1",
            StatementResult.CreateUpdateCount(1));
    }

    private string ConnectionString =>
        $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

    private SpannerRetriableConnection CreateConnection()
    {
        var builder = new SpannerConnectionStringBuilder(ConnectionString, ChannelCredentials.Insecure);
        return new SpannerRetriableConnection(new SpannerConnection(builder));
    }

    [Fact]
    public async Task ExecuteNonQuery_SyncAndAsync_ParityWithoutTransaction()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var syncCommand = connection.CreateDmlCommand("UPDATE Foo SET Bar='bar' WHERE Id=1");
        var syncResult = syncCommand.ExecuteNonQuery();
        Assert.Equal(1, syncResult);

        using var asyncCommand = connection.CreateDmlCommand("UPDATE Foo SET Bar='bar' WHERE Id=1");
        var asyncResult = await asyncCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, asyncResult);
    }

    [Fact]
    public async Task ExecuteDbDataReader_SyncAndAsync_ParityWithTimestampBound()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var syncCommand = connection.CreateSelectCommand("SELECT 1");
        syncCommand.TimestampBound = TimestampBound.Strong;
        using var syncReader = syncCommand.ExecuteReader();
        Assert.True(syncReader.Read());
        Assert.Equal(1L, syncReader.GetInt64(0));

        using var asyncCommand = connection.CreateSelectCommand("SELECT 1");
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

        using var command = connection.CreateSelectCommand("SELECT 1");
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

        using var command = connection.CreateSelectCommand("SELECT 1");
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

        using var command = connection.CreateSelectCommand("SELECT 1");
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

        using var command = connection.CreateDmlCommand("UPDATE Foo SET Bar='bar' WHERE Id=1");
        var affectedRows = command.ExecuteNonQuery();
        Assert.Equal(1, affectedRows);
        Assert.Null(command.Transaction);

        // Reusing the command should not throw InvalidOperationException from a disposed transaction
        command.CommandText = "SELECT 1";
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_CommandCanBeReusedAfterExecution()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection.CreateDmlCommand("UPDATE Foo SET Bar='bar' WHERE Id=1");
        var affectedRows = await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, affectedRows);
        Assert.Null(command.Transaction);

        // Reusing the command after async execution should not throw ObjectDisposedException from a disposed transaction
        command.CommandText = "SELECT 1";
        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExecuteScalarAsync_ReusedCommandWithTimestampBoundReset_UpdatesEphemeralTransactionCreationOptions()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection.CreateSelectCommand("SELECT 1");
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

        using var command = connection.CreateDmlCommand("UPDATE Foo SET Bar='bar' WHERE Id=1");
        _fixture.SpannerMock.AbortNextStatement();

        var affectedRows = command.ExecuteNonQuery();
        Assert.Equal(1, affectedRows);
        Assert.Null(command.Transaction);

        command.CommandText = "SELECT 1";
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_StandaloneDml_RetriesOnAbortedAndResetsTransaction()
    {
        using var connection = CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var command = connection.CreateDmlCommand("UPDATE Foo SET Bar='bar' WHERE Id=1");
        _fixture.SpannerMock.AbortNextStatement();

        var affectedRows = await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, affectedRows);
        Assert.Null(command.Transaction);

        command.CommandText = "SELECT 1";
        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
    }
}
