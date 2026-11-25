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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Migrations
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class SpannerHistoryRepositoryTest : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public SpannerHistoryRepositoryTest(SpannerMockServerFixture fixture)
        {
            _fixture = fixture;
            _fixture.SpannerMock.Reset();
            _fixture.DatabaseAdminMock.Reset();
        }

        private static string EOL => Environment.NewLine;

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        [Fact]
        public void GetCreateScript_works()
        {
            using var context = CreateContext();
            var sql = context.GetService<IHistoryRepository>().GetCreateScript();

            Assert.Equal(
                "CREATE TABLE `EFMigrationsHistory` ("
                + EOL
                + "    `MigrationId` STRING(150) NOT NULL,"
                + EOL
                + "    `ProductVersion` STRING(32) NOT NULL"
                + EOL
                + ")PRIMARY KEY (`MigrationId`)",
                sql);
        }

        [Fact]
        public void ExistsSql_And_InterpretExistsResult_ThrowNotSupportedException()
        {
            using var context = CreateContext();
            var repository = new TestableSpannerHistoryRepository(context.GetService<HistoryRepositoryDependencies>());

            Assert.Throws<NotSupportedException>(() => repository.PublicExistsSql);
            Assert.Throws<NotSupportedException>(() => repository.PublicInterpretExistsResult(true));
        }

        private const string ExpectedExistsParameterizedSql =
            "SELECT 1 FROM information_schema.tables WHERE table_schema = @tableSchema AND table_name = @tableName LIMIT 1";

        [Theory]
        [InlineData(null, "EFMigrationsHistory", true)]
        [InlineData("custom'schema", "custom\\'\nHistoryTable", true)]
        [InlineData("\u0001abcdef", "t\u0085\u2028\u2029", false)]
        [InlineData(@"Café'\🎵", "s\0\a\b\f\n\r\t\v\x1b\x7f", false)]
        [InlineData("foo' OR '1'='1", "bar\\'\n; DROP TABLE x; --", false)]
        public void Exists_UsesParameterizedQuery(string schema, string tableName, bool expectedExists)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
            _fixture.SpannerMock.AddOrUpdateStatementResult(ExpectedExistsParameterizedSql, CreateExistsResult(expectedExists));

            using var context = CreateContext(tableName, schema, ConnectionString);
            var repository = Assert.IsType<SpannerHistoryRepository>(context.GetService<IHistoryRepository>());

            Assert.Equal(expectedExists, repository.Exists());

            AssertExistsRequest(schema, tableName);
        }

        [Theory]
        [InlineData(null, "EFMigrationsHistory", true)]
        [InlineData("custom'schema", "custom\\'\nHistoryTable", true)]
        [InlineData("\u0001abcdef", "t\u0085\u2028\u2029", false)]
        [InlineData(@"Café'\🎵", "s\0\a\b\f\n\r\t\v\x1b\x7f", false)]
        [InlineData("foo' OR '1'='1", "bar\\'\n; DROP TABLE x; --", false)]
        public async Task ExistsAsync_UsesParameterizedQuery(string schema, string tableName, bool expectedExists)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
            _fixture.SpannerMock.AddOrUpdateStatementResult(ExpectedExistsParameterizedSql, CreateExistsResult(expectedExists));

            await using var context = CreateContext(tableName, schema, ConnectionString);
            var repository = Assert.IsType<SpannerHistoryRepository>(context.GetService<IHistoryRepository>());

            Assert.Equal(expectedExists, await repository.ExistsAsync(TestContext.Current.CancellationToken));

            AssertExistsRequest(schema, tableName);
        }

        [Fact]
        public void Exists_WhenDatabaseDoesNotExist_ReturnsFalseWithoutQueryingInformationSchema()
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(
                "SELECT 1",
                StatementResult.CreateException(MockSpannerService.CreateDatabaseNotFoundException("d1")));

            using var context = CreateContext(connectionString: ConnectionString);
            var repository = Assert.IsType<SpannerHistoryRepository>(context.GetService<IHistoryRepository>());

            Assert.False(repository.Exists());

            var executeRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal("SELECT 1", executeRequest.Sql);
        }

        [Fact]
        public async Task ExistsAsync_WhenDatabaseDoesNotExist_ReturnsFalseWithoutQueryingInformationSchema()
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(
                "SELECT 1",
                StatementResult.CreateException(MockSpannerService.CreateDatabaseNotFoundException("d1")));

            await using var context = CreateContext(connectionString: ConnectionString);
            var repository = Assert.IsType<SpannerHistoryRepository>(context.GetService<IHistoryRepository>());

            Assert.False(await repository.ExistsAsync(TestContext.Current.CancellationToken));

            var executeRequest = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal("SELECT 1", executeRequest.Sql);
        }

        [Fact]
        public void Exists_WithActiveTransaction_ExecutesOutsideTransaction()
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
            _fixture.SpannerMock.AddOrUpdateStatementResult(ExpectedExistsParameterizedSql, CreateExistsResult(true));

            using var context = CreateContext(connectionString: ConnectionString);
            using var transaction = context.Database.BeginTransaction();
            var repository = Assert.IsType<SpannerHistoryRepository>(context.GetService<IHistoryRepository>());

            Assert.True(repository.Exists());

            var existsRequest = AssertExistsRequest(null, SpannerHistoryRepository.DefaultMigrationsHistoryTableName);
            Assert.Null(existsRequest.Transaction);
        }

        [Fact]
        public async Task ExistsAsync_WithActiveTransaction_ExecutesOutsideTransaction()
        {
            var cancellationToken = TestContext.Current.CancellationToken;
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
            _fixture.SpannerMock.AddOrUpdateStatementResult(ExpectedExistsParameterizedSql, CreateExistsResult(true));

            await using var context = CreateContext(connectionString: ConnectionString);
            await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);
            var repository = Assert.IsType<SpannerHistoryRepository>(context.GetService<IHistoryRepository>());

            Assert.True(await repository.ExistsAsync(cancellationToken));

            var existsRequest = AssertExistsRequest(null, SpannerHistoryRepository.DefaultMigrationsHistoryTableName);
            Assert.Null(existsRequest.Transaction);
        }

        [Fact]
        public async Task ExistsAsync_WhenCanceled_ThrowsOperationCanceledException()
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSelect1ResultSet());
            _fixture.SpannerMock.AddOrUpdateStatementResult(ExpectedExistsParameterizedSql, CreateExistsResult(true));

            await using var context = CreateContext(connectionString: ConnectionString);
            var repository = Assert.IsType<SpannerHistoryRepository>(context.GetService<IHistoryRepository>());
            using var cancellationTokenSource = new CancellationTokenSource();
            await cancellationTokenSource.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => repository.ExistsAsync(cancellationTokenSource.Token));
        }

        private ExecuteSqlRequest AssertExistsRequest(string expectedSchema, string expectedTableName)
        {
            var existsRequest = Assert.Single(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request => request.Sql == ExpectedExistsParameterizedSql);
            Assert.Equal(2, existsRequest.ParamTypes.Count);
            Assert.Equal(2, existsRequest.Params.Fields.Count);
            Assert.Equal(TypeCode.String, existsRequest.ParamTypes["tableSchema"].Code);
            Assert.Equal(expectedSchema ?? string.Empty, existsRequest.Params.Fields["tableSchema"].StringValue);
            Assert.Equal(TypeCode.String, existsRequest.ParamTypes["tableName"].Code);
            Assert.Equal(expectedTableName, existsRequest.Params.Fields["tableName"].StringValue);
            return existsRequest;
        }

        private static StatementResult CreateExistsResult(bool exists)
            => exists
                ? StatementResult.CreateSelect1ResultSet()
                : StatementResult.CreateResultSet([Tuple.Create(TypeCode.Int64, "COL1")], []);

        private static DbContext CreateContext(
            string tableName = SpannerHistoryRepository.DefaultMigrationsHistoryTableName,
            string schema = null,
            string connectionString = "Data Source=projects/p1/instances/i1/databases/d1")
            => new(
                new DbContextOptionsBuilder()
                    .UseInternalServiceProvider(SpannerTestHelpers.Instance.CreateServiceProvider())
                    .UseSpanner(
                        connectionString,
                        builder =>
                        {
                            SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false);
                            builder.MigrationsHistoryTable(tableName, schema);
                        },
                        ChannelCredentials.Insecure)
                    .Options);

        private sealed class TestableSpannerHistoryRepository(HistoryRepositoryDependencies dependencies)
            : SpannerHistoryRepository(dependencies)
        {
            public string PublicExistsSql => ExistsSql;

            public bool PublicInterpretExistsResult(object value) => InterpretExistsResult(value);
        }
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
