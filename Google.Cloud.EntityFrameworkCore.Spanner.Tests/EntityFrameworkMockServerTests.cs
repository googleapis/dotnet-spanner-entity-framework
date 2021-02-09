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
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
    internal class MockServerSampleDbContext : SpannerSampleDbContext
    {
        private readonly string _connectionString;

        internal MockServerSampleDbContext(string connectionString) : base()
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner(new SpannerConnection(_connectionString, ChannelCredentials.Insecure))
                    .UseLazyLoadingProxies();
            }
        }
    }

    internal class MockServerVersionDbContext : SpannerVersionDbContext
    {
        private readonly string _connectionString;

        internal MockServerVersionDbContext(string connectionString) : base()
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner(new SpannerConnection(_connectionString, ChannelCredentials.Insecure))
                    .UseLazyLoadingProxies();
            }
        }
    }

    /// <summary>
    /// Tests CRUD operations using an in-mem Spanner mock server.
    /// </summary>
    public class EntityFrameworkMockServerTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public EntityFrameworkMockServerTests(SpannerMockServerFixture service)
        {
            _fixture = service;
            service.SpannerMock.Reset();
        }

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        [Fact]
        public async Task FindSingerAsync_ReturnsNull_IfNotFound()
        {
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE s.SingerId = @__p_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.Type, string>> { },
                new List<object[]> { }
            ));

            using var db = new MockServerSampleDbContext(ConnectionString);
            var singer = await db.Singers.FindAsync(1L);
            Assert.Null(singer);
        }

        [Fact]
        public async Task FindSingerAsync_ReturnsInstance_IfFound()
        {
            var sql = AddFindSingerResult($"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE s.SingerId = @__p_0{Environment.NewLine}LIMIT 1");

            using var db = new MockServerSampleDbContext(ConnectionString);
            var singer = await db.Singers.FindAsync(1L);
            Assert.Equal(1L, singer.SingerId);
            Assert.Null(singer.BirthDate);
            Assert.Equal("Alice", singer.FirstName);
            Assert.Equal("Alice Morrison", singer.FullName);
            Assert.Equal("Morrison", singer.LastName);
            Assert.Null(singer.Picture);

            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is V1.ExecuteSqlRequest).Select(request => (V1.ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Null(request.Transaction);
                }
            );
            // A read-only operation should not initiate and commit a transaction.
            Assert.Empty(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));
        }

        [Fact]
        public async Task InsertSinger_SelectsFullName()
        {
            // Setup results.
            var insertSql = $"INSERT INTO Singers (SingerId, BirthDate, FirstName, LastName, Picture){Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Morrison", 0);

            using var db = new MockServerSampleDbContext(ConnectionString);
            db.Singers.Add(new Singers
            {
                SingerId = 1L,
                FirstName = "Alice",
                LastName = "Morrison",
            });
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is V1.ExecuteBatchDmlRequest).Select(request => (V1.ExecuteBatchDmlRequest)request),
                request =>
                {
                    Assert.Single(request.Statements);
                    Assert.Equal(insertSql, request.Statements[0].Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is V1.ExecuteSqlRequest).Select(request => (V1.ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));
        }

        [Fact]
        public async Task UpdateSinger_SelectsFullName()
        {
            // Setup results.
            var updateSql = $"UPDATE Singers SET LastName = @p0{Environment.NewLine}WHERE SingerId = @p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));
            var selectSingerSql = AddFindSingerResult($"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE s.SingerId = @__p_0{Environment.NewLine}LIMIT 1");
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Pieterson-Morrison", 1);

            using var db = new MockServerSampleDbContext(ConnectionString);
            var singer = await db.Singers.FindAsync(1L);
            singer.LastName = "Pieterson-Morrison";
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is V1.ExecuteBatchDmlRequest).Select(request => (V1.ExecuteBatchDmlRequest)request),
                request =>
                {
                    Assert.Single(request.Statements);
                    Assert.Equal(updateSql, request.Statements[0].Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is V1.ExecuteSqlRequest).Select(request => (V1.ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(selectSingerSql, request.Sql);
                    Assert.Null(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));
        }

        [Fact]
        public async Task DeleteSinger_DoesNotSelectFullName()
        {
            // Setup results.
            var deleteSql = $"DELETE FROM Singers{Environment.NewLine}WHERE SingerId = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(deleteSql, StatementResult.CreateUpdateCount(1L));

            using var db = new MockServerSampleDbContext(ConnectionString);
            db.Singers.Remove(new Singers { SingerId = 1L });
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is V1.ExecuteBatchDmlRequest).Select(request => (V1.ExecuteBatchDmlRequest)request),
                request =>
                {
                    Assert.Single(request.Statements);
                    Assert.Equal(deleteSql, request.Statements[0].Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Empty(_fixture.SpannerMock.Requests.Where(request => request is V1.ExecuteSqlRequest));
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));
        }

        [Fact]
        public async Task CanUseReadOnlyTransaction()
        {
            var sql = AddFindSingerResult($"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE s.SingerId = @__p_0{Environment.NewLine}LIMIT 1");
            using var db = new MockServerSampleDbContext(ConnectionString);
            using var transaction = await db.Database.BeginReadOnlyTransactionAsync();

            Assert.NotNull(await db.Singers.FindAsync(1L));

            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is V1.ExecuteSqlRequest).Select(request => (V1.ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests
                .Where(request => request is V1.BeginTransactionRequest)
                .Select(request => (V1.BeginTransactionRequest)request)
                .Where(request => request.Options?.ReadOnly?.Strong ?? false));
        }

        [Fact]
        public async Task CanUseReadOnlyTransactionWithTimestampBound()
        {
            var sql = AddFindSingerResult($"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE s.SingerId = @__p_0{Environment.NewLine}LIMIT 1");
            using var db = new MockServerSampleDbContext(ConnectionString);
            using var transaction = await db.Database.BeginReadOnlyTransactionAsync(TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(10)));

            Assert.NotNull(await db.Singers.FindAsync(1L));

            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is V1.ExecuteSqlRequest).Select(request => (V1.ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests
                .Where(request => request is V1.BeginTransactionRequest)
                .Select(request => (V1.BeginTransactionRequest)request)
                .Where(request => request.Options?.ReadOnly?.ExactStaleness?.Seconds == 10L));
        }

        [Fact]
        public async Task InsertUsingRawSqlReturnsUpdateCountWithoutAdditionalSelectCommand()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var today = SpannerDate.FromDateTime(DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Unspecified));
            var now = DateTime.UtcNow;
            var id = 1L;
            var rawSql = @$"INSERT INTO TableWithAllColumnTypes 
                              (ColBool, ColBoolArray, ColBytes, ColBytesMax, ColBytesArray, ColBytesMaxArray,
                               ColDate, ColDateArray, ColFloat64, ColFloat64Array, ColInt64, ColInt64Array,
                               ColNumeric, ColNumericArray, ColString, ColStringArray, ColStringMax, ColStringMaxArray,
                               ColTimestamp, ColTimestampArray)
                              VALUES
                              (@ColBool, @ColBoolArray, @ColBytes, @ColBytesMax, @ColBytesArray, @ColBytesMaxArray,
                               @ColDate, @ColDateArray, @ColFloat64, @ColFloat64Array, @ColInt64, @ColInt64Array,
                               @ColNumeric, @ColNumericArray, @ColString, @ColStringArray, @ColStringMax, @ColStringMaxArray,
                               @ColTimestamp, @ColTimestampArray)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(rawSql, StatementResult.CreateUpdateCount(1L));

            var row = new TableWithAllColumnTypes
            {
                ColBool = true,
                ColBoolArray = new List<bool?> { true, false, true },
                ColBytes = new byte[] { 1, 2, 3 },
                ColBytesMax = Encoding.UTF8.GetBytes("This is a long string"),
                ColBytesArray = new List<byte[]> { new byte[] { 3, 2, 1 }, new byte[] { }, new byte[] { 4, 5, 6 } },
                ColBytesMaxArray = new List<byte[]> { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2"), Encoding.UTF8.GetBytes("string 3") },
                ColDate = new SpannerDate(2020, 12, 28),
                ColDateArray = new List<SpannerDate?> { new SpannerDate(2020, 12, 28), new SpannerDate(2010, 1, 1), today },
                ColFloat64 = 3.14D,
                ColFloat64Array = new List<double?> { 3.14D, 6.626D },
                ColInt64 = id,
                ColInt64Array = new List<long?> { 1L, 2L, 4L, 8L },
                ColNumeric = (SpannerNumeric?)3.14m,
                ColNumericArray = new List<SpannerNumeric?> { (SpannerNumeric)3.14m, (SpannerNumeric)6.626m },
                ColString = "some string",
                ColStringArray = new List<string> { "string1", "string2", "string3" },
                ColStringMax = "some longer string",
                ColStringMaxArray = new List<string> { "longer string1", "longer string2", "longer string3" },
                ColTimestamp = new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288),
                ColTimestampArray = new List<DateTime?> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now },
            };
            var updateCount = await db.Database.ExecuteSqlRawAsync(rawSql,
                new SpannerParameter("ColBool", SpannerDbType.Bool, row.ColBool),
                new SpannerParameter("ColBoolArray", SpannerDbType.ArrayOf(SpannerDbType.Bool), row.ColBoolArray),
                new SpannerParameter("ColBytes", SpannerDbType.Bytes, row.ColBytes),
                new SpannerParameter("ColBytesMax", SpannerDbType.Bytes, row.ColBytesMax),
                new SpannerParameter("ColBytesArray", SpannerDbType.ArrayOf(SpannerDbType.Bytes), row.ColBytesArray),
                new SpannerParameter("ColBytesMaxArray", SpannerDbType.ArrayOf(SpannerDbType.Bytes), row.ColBytesMaxArray),
                new SpannerParameter("ColDate", SpannerDbType.Date, row.ColDate),
                new SpannerParameter("ColDateArray", SpannerDbType.ArrayOf(SpannerDbType.Date), row.ColDateArray),
                new SpannerParameter("ColFloat64", SpannerDbType.Float64, row.ColFloat64),
                new SpannerParameter("ColFloat64Array", SpannerDbType.ArrayOf(SpannerDbType.Float64), row.ColFloat64Array),
                new SpannerParameter("ColInt64", SpannerDbType.Int64, row.ColInt64),
                new SpannerParameter("ColInt64Array", SpannerDbType.ArrayOf(SpannerDbType.Int64), row.ColInt64Array),
                new SpannerParameter("ColNumeric", SpannerDbType.Numeric, row.ColNumeric),
                new SpannerParameter("ColNumericArray", SpannerDbType.ArrayOf(SpannerDbType.Numeric), row.ColNumericArray),
                new SpannerParameter("ColString", SpannerDbType.String, row.ColString),
                new SpannerParameter("ColStringArray", SpannerDbType.ArrayOf(SpannerDbType.String), row.ColStringArray),
                new SpannerParameter("ColStringMax", SpannerDbType.String, row.ColStringMax),
                new SpannerParameter("ColStringMaxArray", SpannerDbType.ArrayOf(SpannerDbType.String), row.ColStringMaxArray),
                new SpannerParameter("ColTimestamp", SpannerDbType.Timestamp, row.ColTimestamp),
                new SpannerParameter("ColTimestampArray", SpannerDbType.ArrayOf(SpannerDbType.Timestamp), row.ColTimestampArray)
            );

            Assert.Equal(1, updateCount);
            // Verify that the INSERT statement is the only one on the mock server.
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(r => r is ExecuteSqlRequest sqlRequest).Select(r => r as ExecuteSqlRequest),
                request => Assert.Equal(rawSql, request.Sql)
            );
        }

        [Fact]
        public async Task VersionNumberIsAutomaticallyGeneratedOnInsertAndUpdate()
        {
            var insertSql = $"INSERT INTO SingersWithVersion (SingerId, FirstName, LastName, Version){Environment.NewLine}VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            using var db = new MockServerVersionDbContext(ConnectionString);
            var singer = new SingersWithVersion { SingerId = 1L, FirstName = "Pete", LastName = "Allison" };
            db.Singers.Add(singer);
            await db.SaveChangesAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(r => r is ExecuteBatchDmlRequest).Select(r => r as ExecuteBatchDmlRequest),
                batchRequest =>
                {
                    Assert.Single(batchRequest.Statements);
                    var statement = batchRequest.Statements[0];
                    Assert.Equal(insertSql, statement.Sql);
                    Assert.Equal(Value.ForString("1"), statement.Params.Fields["p0"]);
                    Assert.Equal(Value.ForString("Pete"), statement.Params.Fields["p1"]);
                    Assert.Equal(Value.ForString("Allison"), statement.Params.Fields["p2"]);
                    // Verify that the version is 1.
                    Assert.Equal(Value.ForString("1"), statement.Params.Fields["p3"]);
                }
            );

            _fixture.SpannerMock.Reset();
            // Update the singer and verify that the version number is included in the WHERE clause and is updated.
            var updateSql = $"UPDATE SingersWithVersion SET LastName = @p0, Version = @p1{Environment.NewLine}WHERE SingerId = @p2 AND Version = @p3";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));

            singer.LastName = "Peterson - Allison";
            await db.SaveChangesAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(r => r is ExecuteBatchDmlRequest).Select(r => r as ExecuteBatchDmlRequest),
                batchRequest =>
                {
                    Assert.Single(batchRequest.Statements);
                    var statement = batchRequest.Statements[0];
                    Assert.Equal(updateSql, statement.Sql);
                    Assert.Equal(Value.ForString("Peterson - Allison"), statement.Params.Fields["p0"]);
                    Assert.Equal(Value.ForString("1"), statement.Params.Fields["p2"]);

                    // Verify that the version that is set is 2.
                    Assert.Equal(Value.ForString("2"), statement.Params.Fields["p1"]);
                    // Verify that the version that is checked is 1.
                    Assert.Equal(Value.ForString("1"), statement.Params.Fields["p3"]);
                }
            );
        }

        [Fact]
        public async Task UpdateFailsIfVersionNumberChanged()
        {
            var updateSql = $"UPDATE SingersWithVersion SET LastName = @p0, Version = @p1{Environment.NewLine}WHERE SingerId = @p2 AND Version = @p3";
            // Set the update count to 0 to indicate that the row was not found.
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(0L));
            using var db = new MockServerVersionDbContext(ConnectionString);

            // Attach a singer to the context and try to update it.
            var singer = new SingersWithVersion { SingerId = 1L, FirstName = "Pete", LastName = "Allison", Version = 1L };
            db.Attach(singer);

            singer.LastName = "Allison - Peterson";
            await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync());

            // Update the update count to 1 to simulate a resolved version conflict.
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));
            Assert.Equal(1L, await db.SaveChangesAsync());
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task ExplicitAndImplicitTransactionIsRetried(bool disableInternalRetries, bool useExplicitTransaction)
        {
            // Setup results.
            var insertSql = $"INSERT INTO Venues (Code, Active, Capacity, Name, Ratings){Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            // Abort the next statement that is executed on the mock server.
            _fixture.SpannerMock.AbortNextStatement();

            using var db = new MockServerSampleDbContext(ConnectionString);
            IDbContextTransaction transaction = null;
            if (useExplicitTransaction)
            {
                transaction = await db.Database.BeginTransactionAsync();
                if (disableInternalRetries)
                {
                    transaction.DisableInternalRetries();
                }
            }
            db.Venues.Add(new Venues
            {
                Code = "C1",
                Name = "Concert Hall",
            });

            // We can only disable internal retries when using explicit transactions. Otherwise internal retries
            // are always used.
            if (disableInternalRetries && useExplicitTransaction)
            {
                var e = await Assert.ThrowsAsync<SpannerException>(() => db.SaveChangesAsync());
                Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            }
            else
            {
                var updateCount = await db.SaveChangesAsync();
                Assert.Equal(1L, updateCount);
                if (useExplicitTransaction)
                {
                    await transaction.CommitAsync();
                }
                Assert.Collection(
                    _fixture.SpannerMock.Requests.Where(request => request is ExecuteBatchDmlRequest).Select(request => (ExecuteBatchDmlRequest)request),
                    // The Batch DML request is sent twice to the server, as the statement is aborted during the first attempt.
                    request =>
                    {
                        Assert.Single(request.Statements);
                        Assert.Equal(insertSql, request.Statements[0].Sql);
                        Assert.NotNull(request.Transaction?.Id);
                    },
                    request =>
                    {
                        Assert.Single(request.Statements);
                        Assert.Equal(insertSql, request.Statements[0].Sql);
                        Assert.NotNull(request.Transaction?.Id);
                    }
                );
                // Even if we are using implicit transactions, there will still be a transaction in the background and this transaction should be committed.
                Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is CommitRequest));
            }
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task ExplicitAndImplicitTransactionIsRetried_WhenUsingRawSql(bool disableInternalRetries, bool useExplicitTransaction)
        {
            // Setup results.
            var insertSql = $"INSERT INTO Venues (Code, Active, Capacity, Name, Ratings){Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            // Abort the next statement that is executed on the mock server.
            _fixture.SpannerMock.AbortNextStatement();

            using var db = new MockServerSampleDbContext(ConnectionString);
            IDbContextTransaction transaction = null;
            if (useExplicitTransaction)
            {
                transaction = await db.Database.BeginTransactionAsync();
                if (disableInternalRetries)
                {
                    transaction.DisableInternalRetries();
                }
            }

            // We can only disable internal retries when using explicit transactions. Otherwise internal retries
            // are always used.
            if (disableInternalRetries && useExplicitTransaction)
            {
                var e = await Assert.ThrowsAsync<SpannerException>(() => db.Database.ExecuteSqlRawAsync(insertSql,
                    new SpannerParameter("p0", SpannerDbType.String, "C1"),
                    new SpannerParameter("p1", SpannerDbType.Bool, true),
                    new SpannerParameter("p2", SpannerDbType.Int64, 1000L),
                    new SpannerParameter("p3", SpannerDbType.String, "Concert Hall"),
                    new SpannerParameter("p4", SpannerDbType.ArrayOf(SpannerDbType.Float64))
                ));
                Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
            }
            else
            {
                var updateCount = await db.Database.ExecuteSqlRawAsync(insertSql,
                    new SpannerParameter("p0", SpannerDbType.String, "C1"),
                    new SpannerParameter("p1", SpannerDbType.Bool, true),
                    new SpannerParameter("p2", SpannerDbType.Int64, 1000L),
                    new SpannerParameter("p3", SpannerDbType.String, "Concert Hall"),
                    new SpannerParameter("p4", SpannerDbType.ArrayOf(SpannerDbType.Float64))
                );
                Assert.Equal(1L, updateCount);
                if (useExplicitTransaction)
                {
                    await transaction.CommitAsync();
                }
                Assert.Collection(
                    _fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest).Select(request => (ExecuteSqlRequest)request),
                    // The ExecuteSqlRequest is sent twice to the server, as the statement is aborted during the first attempt.
                    request =>
                    {
                        Assert.Equal(insertSql, request.Sql);
                        Assert.NotNull(request.Transaction?.Id);
                    },
                    request =>
                    {
                        Assert.Equal(insertSql, request.Sql);
                        Assert.NotNull(request.Transaction?.Id);
                    }
                );
                // Even if we are using implicit transactions, there will still be a transaction in the background and this transaction should be committed.
                Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is CommitRequest));
            }
        }

        [Fact]
        public async Task CanUseLimitWithoutOffset()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}ORDER BY s.LastName{Environment.NewLine}LIMIT @__p_0";
            AddFindSingerResult(sql);

            var singers = await db.Singers
                .OrderBy(s => s.LastName)
                .Take(1)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest).Select(request => (ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal("1", request.Params.Fields["__p_0"].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanUseLimitWithOffset()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}ORDER BY s.LastName{Environment.NewLine}LIMIT @__p_1 OFFSET @__p_0";
            AddFindSingerResult(sql);

            var singers = await db.Singers
                .OrderBy(s => s.LastName)
                .Skip(2)
                .Take(1)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest).Select(request => (ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal("2", request.Params.Fields["__p_0"].StringValue);
                    Assert.Equal("1", request.Params.Fields["__p_1"].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanUseOffsetWithoutLimit()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}ORDER BY s.LastName{Environment.NewLine}LIMIT {long.MaxValue / 2} OFFSET @__p_0";
            AddFindSingerResult(sql);

            var singers = await db.Singers
                .OrderBy(s => s.LastName)
                .Skip(3)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest).Select(request => (ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal("3", request.Params.Fields["__p_0"].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanUseInnerJoin()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture, a.AlbumId, a.ReleaseDate, a.SingerId, a.Title{Environment.NewLine}FROM Singers AS s{Environment.NewLine}INNER JOIN Albums AS a ON s.SingerId = a.SingerId";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.Date, "BirthDate"),
                    Tuple.Create(V1.TypeCode.String, "FirstName"),
                    Tuple.Create(V1.TypeCode.String, "FullName"),
                    Tuple.Create(V1.TypeCode.String, "LastName"),
                    Tuple.Create(V1.TypeCode.Bytes, "Picture"),
                    Tuple.Create(V1.TypeCode.Int64, "AlbumId"),
                    Tuple.Create(V1.TypeCode.Date, "ReleaseDate"),
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.String, "Title"),
                },
                new List<object[]>
                {
                    new object[] { 1L, null, "Zeke", "Zeke Peterson", "Peterson", null, 100L, null, 1L, "Some Title" },
                }
            ));

            var singers = await db.Singers
                .Join(db.Albums, a => a.SingerId, s => s.SingerId, (s, a) => new { Singer = s, Album = a })
                .ToListAsync();

            Assert.Collection(singers,
                s =>
                {
                    Assert.Equal("Peterson", s.Singer.LastName);
                    Assert.Equal("Some Title", s.Album.Title);
                }
            );
        }

        [Fact]
        public async Task CanUseOuterJoin()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture, a.AlbumId, a.ReleaseDate, a.SingerId, a.Title{Environment.NewLine}FROM Singers AS s{Environment.NewLine}LEFT JOIN Albums AS a ON s.SingerId = a.SingerId";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.Date, "BirthDate"),
                    Tuple.Create(V1.TypeCode.String, "FirstName"),
                    Tuple.Create(V1.TypeCode.String, "FullName"),
                    Tuple.Create(V1.TypeCode.String, "LastName"),
                    Tuple.Create(V1.TypeCode.Bytes, "Picture"),
                    Tuple.Create(V1.TypeCode.Int64, "AlbumId"),
                    Tuple.Create(V1.TypeCode.Date, "ReleaseDate"),
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.String, "Title"),
                },
                new List<object[]>
                {
                    new object[] { 2L, null, "Alice", "Alice Morrison", "Morrison", null, null, null, null, null },
                    new object[] { 3L, null, "Zeke", "Zeke Peterson", "Peterson", null, 100L, null, 3L, "Some Title" },
                }
            ));

            var singers = await db.Singers
                .GroupJoin(db.Albums, s => s.SingerId, a => a.SingerId, (s, a) => new { Singer = s, Albums = a })
                .SelectMany(
                    s => s.Albums.DefaultIfEmpty(),
                    (s, a) => new { s.Singer, Album = a })
                .ToListAsync();

            Assert.Collection(singers,
                s =>
                {
                    Assert.Equal("Morrison", s.Singer.LastName);
                    Assert.Null(s.Album);
                },
                s =>
                {
                    Assert.Equal("Peterson", s.Singer.LastName);
                    Assert.Equal(100L, s.Album.AlbumId);
                    Assert.Equal("Some Title", s.Album.Title);
                }
            );
        }

        [Fact]
        public async Task CanUseStringContains()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE STRPOS(s.FirstName, @__firstName_0) > 0";
            AddFindSingerResult(sql);

            var firstName = "Alice";
            var singers = await db.Singers
                .Where(s => s.FirstName.Contains(firstName))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest).Select(request => (ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal("Alice", request.Params.Fields["__firstName_0"].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanUseStringStartsWith()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE (@__fullName_0 = '') OR STARTS_WITH(s.FullName, @__fullName_0)";
            AddFindSingerResult(sql);

            var fullName = "Alice M";
            var singers = await db.Singers
                .Where(s => s.FullName.StartsWith(fullName))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest).Select(request => (ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal("Alice M", request.Params.Fields["__fullName_0"].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanUseStringEndsWith()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE (@__fullName_0 = '') OR ENDS_WITH(s.FullName, @__fullName_0)";
            AddFindSingerResult(sql);

            var fullName = " Morrison";
            var singers = await db.Singers
                .Where(s => s.FullName.EndsWith(fullName))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest).Select(request => (ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal(" Morrison", request.Params.Fields["__fullName_0"].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanUseStringLength()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE CHAR_LENGTH(s.FirstName) > @__minLength_0";
            AddFindSingerResult(sql);

            var minLength = 4;
            var singers = await db.Singers
                .Where(s => s.FirstName.Length > minLength)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest).Select(request => (ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal("4", request.Params.Fields["__minLength_0"].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanUseRegexReplace()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT REGEXP_REPLACE(s.FirstName, @__regex_1, @__replacement_2){Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE s.SingerId = @__singerId_0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "FirstName"),
                },
                new List<object[]>
                {
                    new object[] { "Allison" },
                }
            ));

            var singerId = 1L;
            var replacement = "Allison";
            var pattern = "Al.*";
            var regex = new Regex(pattern);
            var firstNames = await db.Singers
                .Where(s => s.SingerId == singerId)
                .Select(s => regex.Replace(s.FirstName, replacement))
                .ToListAsync();
            Assert.Collection(firstNames, s => Assert.Equal("Allison", s));
        }

        [Fact]
        public async Task CanUseDateTimeAddYears()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            // Note: AddYears cannot be applied server side to a TIMESTAMP, only to a DATE, so this is handled client side.
            var sql = $"SELECT c.StartTime{Environment.NewLine}FROM Concerts AS c{Environment.NewLine}WHERE c.SingerId = @__singerId_0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "StartTime"),
                },
                new List<object[]>
                {
                    new object[] { "2021-01-20T18:00:00Z" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Concerts
                .Where(c => c.SingerId == singerId)
                .Select(s => s.StartTime.AddYears(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new DateTime(2022, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
        }

        [Fact]
        public async Task CanUseSpannerDateAddYears()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT DATE_ADD(s.BirthDate, INTERVAL 1 YEAR){Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE (s.SingerId = @__singerId_0) AND s.BirthDate IS NOT NULL";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "BirthDate"),
                },
                new List<object[]>
                {
                    new object[] { "1980-01-20" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Singers
                .Where(s => s.SingerId == singerId && s.BirthDate != null)
                .Select(s => ((SpannerDate)s.BirthDate).AddYears(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new SpannerDate(1980, 1, 20), s));
        }

        [Fact]
        public async Task CanUseDateTimeAddMonths()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            // Note: AddMonths cannot be applied server side to a TIMESTAMP, only to a DATE, so this is handled client side.
            var sql = $"SELECT c.StartTime{Environment.NewLine}FROM Concerts AS c{Environment.NewLine}WHERE c.SingerId = @__singerId_0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "StartTime"),
                },
                new List<object[]>
                {
                    new object[] { "2021-01-20T18:00:00Z" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Concerts
                .Where(c => c.SingerId == singerId)
                .Select(s => s.StartTime.AddMonths(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new DateTime(2021, 2, 20, 18, 0, 0, DateTimeKind.Utc), s));
        }

        [Fact]
        public async Task CanUseSpannerDateAddMonths()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT DATE_ADD(s.BirthDate, INTERVAL 1 MONTH){Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE (s.SingerId = @__singerId_0) AND s.BirthDate IS NOT NULL";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "BirthDate"),
                },
                new List<object[]>
                {
                    new object[] { "1980-01-20" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Singers
                .Where(s => s.SingerId == singerId && s.BirthDate != null)
                .Select(s => ((SpannerDate)s.BirthDate).AddMonths(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new SpannerDate(1980, 1, 20), s));
        }

        [Fact]
        public async Task CanUseDateTimeAddDays()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT TIMESTAMP_ADD(c.StartTime, INTERVAL CAST(1.0 AS INT64) DAY){Environment.NewLine}FROM Concerts AS c{Environment.NewLine}WHERE c.SingerId = @__singerId_0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "StartTime"),
                },
                new List<object[]>
                {
                    new object[] { "2021-01-20T18:00:00Z" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Concerts
                .Where(c => c.SingerId == singerId)
                .Select(s => s.StartTime.AddDays(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
        }

        [Fact]
        public async Task CanUseSpannerDateAddDays()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT DATE_ADD(s.BirthDate, INTERVAL 1 DAY){Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE (s.SingerId = @__singerId_0) AND s.BirthDate IS NOT NULL";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "BirthDate"),
                },
                new List<object[]>
                {
                    new object[] { "1980-01-20" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Singers
                .Where(s => s.SingerId == singerId && s.BirthDate != null)
                .Select(s => ((SpannerDate)s.BirthDate).AddDays(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new SpannerDate(1980, 1, 20), s));
        }

        [Fact]
        public async Task CanUseDateTimeAddHours()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT TIMESTAMP_ADD(c.StartTime, INTERVAL CAST(1.0 AS INT64) HOUR){Environment.NewLine}FROM Concerts AS c{Environment.NewLine}WHERE c.SingerId = @__singerId_0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "StartTime"),
                },
                new List<object[]>
                {
                    new object[] { "2021-01-20T18:00:00Z" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Concerts
                .Where(c => c.SingerId == singerId)
                .Select(s => s.StartTime.AddHours(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
        }

        [Fact]
        public async Task CanUseDateTimeAddMinutes()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT TIMESTAMP_ADD(c.StartTime, INTERVAL CAST(1.0 AS INT64) MINUTE){Environment.NewLine}FROM Concerts AS c{Environment.NewLine}WHERE c.SingerId = @__singerId_0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "StartTime"),
                },
                new List<object[]>
                {
                    new object[] { "2021-01-20T18:00:00Z" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Concerts
                .Where(c => c.SingerId == singerId)
                .Select(s => s.StartTime.AddMinutes(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
        }

        [Fact]
        public async Task CanUseDateTimeAddSeconds()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT TIMESTAMP_ADD(c.StartTime, INTERVAL CAST(1.0 AS INT64) SECOND){Environment.NewLine}FROM Concerts AS c{Environment.NewLine}WHERE c.SingerId = @__singerId_0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "StartTime"),
                },
                new List<object[]>
                {
                    new object[] { "2021-01-20T18:00:00Z" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Concerts
                .Where(c => c.SingerId == singerId)
                .Select(s => s.StartTime.AddSeconds(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
        }

        [Fact]
        public async Task CanUseDateTimeAddMilliseconds()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT TIMESTAMP_ADD(c.StartTime, INTERVAL CAST(1.0 AS INT64) MILLISECOND){Environment.NewLine}FROM Concerts AS c{Environment.NewLine}WHERE c.SingerId = @__singerId_0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "StartTime"),
                },
                new List<object[]>
                {
                    new object[] { "2021-01-20T18:00:00Z" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Concerts
                .Where(c => c.SingerId == singerId)
                .Select(s => s.StartTime.AddMilliseconds(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
        }

        [Fact]
        public async Task CanUseDateTimeAddTicks()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT TIMESTAMP_ADD(c.StartTime, INTERVAL 100 * 1 NANOSECOND){Environment.NewLine}FROM Concerts AS c{Environment.NewLine}WHERE c.SingerId = @__singerId_0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Timestamp, "StartTime"),
                },
                new List<object[]>
                {
                    new object[] { "2021-01-20T18:00:00Z" },
                }
            ));

            var singerId = 1L;
            var startTimes = await db.Concerts
                .Where(c => c.SingerId == singerId)
                .Select(s => s.StartTime.AddTicks(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
        }

        [Fact]
        public async Task CanUseLongAbs()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT ABS(t.ColInt64){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "ColInt64"),
                },
                new List<object[]>
                {
                    new object[] { "1" },
                }
            ));

            var id = -1L;
            var absId = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Abs(t.ColInt64))
                .FirstOrDefaultAsync();
            Assert.Equal(1L, absId);
        }

        [Fact]
        public async Task CanUseDoubleAbs()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT ABS(COALESCE(t.ColFloat64, 0.0)){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Float64, "ColFloat64"),
                },
                new List<object[]>
                {
                    new object[] { "3.14" },
                }
            ));

            var id = -1L;
            var absId = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Abs(t.ColFloat64.GetValueOrDefault()))
                .FirstOrDefaultAsync();
            Assert.Equal(3.14d, absId);
        }

        [Fact]
        public async Task CanUseDecimalAbs()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT ABS(COALESCE(t.ColNumeric, 0)){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Numeric, "ColNumeric"),
                },
                new List<object[]>
                {
                    new object[] { "3.14" },
                }
            ));

            var id = -1L;
            var absId = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Abs(t.ColNumeric.GetValueOrDefault().ToDecimal(LossOfPrecisionHandling.Truncate)))
                .FirstOrDefaultAsync();
            Assert.Equal(3.14m, absId);
        }

        [Fact]
        public async Task CanUseLongMax()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT GREATEST(t.ColInt64, 2){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "ColInt64"),
                },
                new List<object[]>
                {
                    new object[] { "2" },
                }
            ));

            var id = 1L;
            var max = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Max(t.ColInt64, 2L))
                .FirstOrDefaultAsync();
            Assert.Equal(2L, max);
        }

        [Fact]
        public async Task CanUseDoubleMax()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT GREATEST(COALESCE(t.ColFloat64, 0.0), 3.1400000000000001){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Float64, "ColFloat64"),
                },
                new List<object[]>
                {
                    new object[] { "3.1400000000000001" },
                }
            ));

            var id = 1L;
            var max = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Max(t.ColFloat64.GetValueOrDefault(), 3.14d))
                .FirstOrDefaultAsync();
            Assert.Equal(3.14d, max);
        }

        [Fact]
        public async Task CanUseLongMin()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT LEAST(t.ColInt64, 2){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "ColInt64"),
                },
                new List<object[]>
                {
                    new object[] { "1" },
                }
            ));

            var id = 1L;
            var min = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Min(t.ColInt64, 2L))
                .FirstOrDefaultAsync();
            Assert.Equal(1L, min);
        }

        [Fact]
        public async Task CanUseDoubleMin()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT LEAST(COALESCE(t.ColFloat64, 0.0), 3.1400000000000001){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Float64, "ColFloat64"),
                },
                new List<object[]>
                {
                    new object[] { "0.1" },
                }
            ));

            var id = 1L;
            var min = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Min(t.ColFloat64.GetValueOrDefault(), 3.14d))
                .FirstOrDefaultAsync();
            Assert.Equal(0.1d, min);
        }

        [Fact]
        public async Task CanUseRound()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT ROUND(COALESCE(t.ColFloat64, 0.0)){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Float64, "ColFloat64"),
                },
                new List<object[]>
                {
                    new object[] { "3.0" },
                }
            ));

            var id = 1L;
            var rounded = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Round(t.ColFloat64.GetValueOrDefault(), MidpointRounding.AwayFromZero))
                .FirstOrDefaultAsync();
            Assert.Equal(3.0d, rounded);
        }

        [Fact]
        public async Task CanUseDecimalRound()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT ROUND(COALESCE(t.ColNumeric, 0)){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Numeric, "ColNumeric"),
                },
                new List<object[]>
                {
                    new object[] { "3.0" },
                }
            ));

            var id = 1L;
            var rounded = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Round(t.ColNumeric.GetValueOrDefault().ToDecimal(LossOfPrecisionHandling.Truncate), MidpointRounding.AwayFromZero))
                .FirstOrDefaultAsync();
            Assert.Equal(3.0m, rounded);
        }

        [Fact]
        public async Task CanUseRoundWithDigits()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT ROUND(COALESCE(t.ColFloat64, 0.0), 1){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Float64, "ColFloat64"),
                },
                new List<object[]>
                {
                    new object[] { "3.1" },
                }
            ));

            var id = 1L;
            var rounded = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Round(t.ColFloat64.GetValueOrDefault(), 1, MidpointRounding.AwayFromZero))
                .FirstOrDefaultAsync();
            Assert.Equal(3.1d, rounded);
        }

        [Fact]
        public async Task CanUseCeiling()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT CEIL(COALESCE(t.ColFloat64, 0.0)){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Float64, "ColFloat64"),
                },
                new List<object[]>
                {
                    new object[] { "4.0" },
                }
            ));

            var id = 1L;
            var ceil = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Ceiling(t.ColFloat64.GetValueOrDefault()))
                .FirstOrDefaultAsync();
            Assert.Equal(4.0d, ceil);
        }

        [Fact]
        public async Task CanUseFloor()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT FLOOR(COALESCE(t.ColFloat64, 0.0)){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Float64, "ColFloat64"),
                },
                new List<object[]>
                {
                    new object[] { "3.0" },
                }
            ));

            var id = 1L;
            var floor = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => Math.Floor(t.ColFloat64.GetValueOrDefault()))
                .FirstOrDefaultAsync();
            Assert.Equal(3.0d, floor);
        }

        [Fact]
        public async Task CanUseDateTimeProperties()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT EXTRACT(YEAR FROM COALESCE(t.ColTimestamp, TIMESTAMP '0001-01-01T00:00:00Z') AT TIME ZONE 'UTC') AS Year, EXTRACT(MONTH FROM COALESCE(t.ColTimestamp, TIMESTAMP '0001-01-01T00:00:00Z') AT TIME ZONE 'UTC') AS Month{Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "Year"),
                    Tuple.Create(V1.TypeCode.Int64, "Month"),
                },
                new List<object[]>
                {
                    new object[] { "2021" },
                    new object[] { "1" },
                }
            ));

            var id = 1L;
            var extracted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => new
                {
                    t.ColTimestamp.GetValueOrDefault().Year,
                    t.ColTimestamp.GetValueOrDefault().Month,
                })
                .FirstOrDefaultAsync();
            Assert.Equal(2021, extracted.Year);
            Assert.Equal(1, extracted.Month);
        }

        [Fact]
        public async Task CanUseSpannerDateProperties()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT EXTRACT(YEAR FROM COALESCE(t.ColDate, DATE '0001-01-01')) AS Year, EXTRACT(MONTH FROM COALESCE(t.ColDate, DATE '0001-01-01')) AS Month, EXTRACT(DAY FROM COALESCE(t.ColDate, DATE '0001-01-01')) AS Day, EXTRACT(DAYOFYEAR FROM COALESCE(t.ColDate, DATE '0001-01-01')) AS DayOfYear, EXTRACT(DAYOFWEEK FROM COALESCE(t.ColDate, DATE '0001-01-01')) - 1 AS DayOfWeek{Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "Year"),
                    Tuple.Create(V1.TypeCode.Int64, "Month"),
                    Tuple.Create(V1.TypeCode.Int64, "Day"),
                    Tuple.Create(V1.TypeCode.Int64, "DayOfYear"),
                    Tuple.Create(V1.TypeCode.Int64, "DayOfWeek"),
                },
                new List<object[]>
                {
                    new object[] { "2021" },
                    new object[] { "1" },
                    new object[] { "25" },
                    new object[] { "25" },
                    new object[] { "1" },
                }
            ));

            var id = 1L;
            var extracted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => new
                {
                    t.ColDate.GetValueOrDefault().Year,
                    t.ColDate.GetValueOrDefault().Month,
                    t.ColDate.GetValueOrDefault().Day,
                    t.ColDate.GetValueOrDefault().DayOfYear,
                    t.ColDate.GetValueOrDefault().DayOfWeek,
                })
                .FirstOrDefaultAsync();
            Assert.Equal(2021, extracted.Year);
            Assert.Equal(1, extracted.Month);
            Assert.Equal(25, extracted.Day);
            Assert.Equal(25, extracted.DayOfYear);
            Assert.Equal(DayOfWeek.Monday, extracted.DayOfWeek);
        }

        [Fact]
        public async Task CanUseBoolToString()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT CAST(COALESCE(t.ColBool, false) AS STRING){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "Converted"),
                },
                new List<object[]>
                {
                    new object[] { "TRUE" },
                }
            ));
            var id = 1L;
            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColBool.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("TRUE", converted);
        }

        [Fact]
        public async Task CanUseBytesToString()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT CAST(t.ColBytes AS STRING){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "Converted"),
                },
                new List<object[]>
                {
                    new object[] { "some bytes" },
                }
            ));

            var id = 1L;
            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColBytes.ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("some bytes", converted);
        }

        [Fact]
        public async Task CanUseLongToString()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT CAST(t.ColInt64 AS STRING){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "Converted"),
                },
                new List<object[]>
                {
                    new object[] { "100" },
                }
            ));

            var id = 1L;
            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColInt64.ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("100", converted);
        }

        [Fact]
        public async Task CanUseSpannerNumericToString()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT CAST(COALESCE(t.ColNumeric, 0) AS STRING){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "Converted"),
                },
                new List<object[]>
                {
                    new object[] { "3.14" },
                }
            ));

            var id = 1L;
            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColNumeric.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("3.14", converted);
        }

        [Fact]
        public async Task CanUseDoubleToString()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT CAST(COALESCE(t.ColFloat64, 0.0) AS STRING){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "Converted"),
                },
                new List<object[]>
                {
                    new object[] { "3.0" },
                }
            ));

            var id = 1L;
            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColFloat64.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("3.0", converted);
        }

        [Fact]
        public async Task CanUseSpannerDateToString()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT CAST(COALESCE(t.ColDate, DATE '0001-01-01') AS STRING){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "Converted"),
                },
                new List<object[]>
                {
                    new object[] { "2021-01-25" },
                }
            ));

            var id = 1L;
            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColDate.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("2021-01-25", converted);
        }

        [Fact]
        public async Task CanUseDateTimeToString()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT FORMAT_TIMESTAMP('%FT%H:%M:%E*SZ', COALESCE(t.ColTimestamp, TIMESTAMP '0001-01-01T00:00:00Z'), 'UTC'){Environment.NewLine}FROM TableWithAllColumnTypes AS t{Environment.NewLine}WHERE t.ColInt64 = @__id_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "Converted"),
                },
                new List<object[]>
                {
                    new object[] { "2021-01-25T12:46:01.982784Z" },
                }
            ));

            var id = 1L;
            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                .Select(t => t.ColTimestamp.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("2021-01-25T12:46:01.982784Z", converted);
        }

        [Fact]
        public async Task CanInsertCommitTimestamp()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"INSERT INTO TableWithAllColumnTypes (ColCommitTS, ColInt64, ColBool, ColBoolArray, ColBytes, ColBytesArray, ColBytesMax, ColBytesMaxArray, ColDate, ColDateArray, ColFloat64, ColFloat64Array, ColInt64Array, ColNumeric, ColNumericArray, ColString, ColStringArray, ColStringMax, ColStringMaxArray, ColTimestamp, ColTimestampArray){Environment.NewLine}VALUES (PENDING_COMMIT_TIMESTAMP(), @p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));
            _fixture.SpannerMock.AddOrUpdateStatementResult($"{Environment.NewLine}SELECT ColComputed{Environment.NewLine}FROM TableWithAllColumnTypes{Environment.NewLine}WHERE  TRUE  AND ColInt64 = @p0", StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.String }, "FOO"));

            db.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes { ColInt64 = 1L });
            await db.SaveChangesAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteBatchDmlRequest).Select(request => (ExecuteBatchDmlRequest)request),
                request =>
                {
                    Assert.Single(request.Statements);
                    Assert.Contains("PENDING_COMMIT_TIMESTAMP()", request.Statements[0].Sql);
                }
            );
        }

        [Fact]
        public async Task CanUpdateCommitTimestamp()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"UPDATE TableWithAllColumnTypes SET ColCommitTS = PENDING_COMMIT_TIMESTAMP(), ColBool = @p0{Environment.NewLine}WHERE ColInt64 = @p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateUpdateCount(1L));
            _fixture.SpannerMock.AddOrUpdateStatementResult($"{Environment.NewLine}SELECT ColComputed{Environment.NewLine}FROM TableWithAllColumnTypes{Environment.NewLine}WHERE  TRUE  AND ColInt64 = @p1", StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.String }, "FOO"));

            var row = new TableWithAllColumnTypes { ColInt64 = 1L };
            db.TableWithAllColumnTypes.Attach(row);
            row.ColBool = true;
            await db.SaveChangesAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteBatchDmlRequest).Select(request => (ExecuteBatchDmlRequest)request),
                request =>
                {
                    Assert.Single(request.Statements);
                    Assert.Contains("PENDING_COMMIT_TIMESTAMP()", request.Statements[0].Sql);
                }
            );
        }

        private string AddFindSingerResult(string sql)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.Date, "BirthDate"),
                    Tuple.Create(V1.TypeCode.String, "FirstName"),
                    Tuple.Create(V1.TypeCode.String, "FullName"),
                    Tuple.Create(V1.TypeCode.String, "LastName"),
                    Tuple.Create(V1.TypeCode.Bytes, "Picture"),
                },
                new List<object[]>
                {
                    new object[] { 1L, null, "Alice", "Alice Morrison", "Morrison", null },
                }
            ));
            return sql;
        }

        private string AddSelectSingerFullNameResult(string fullName, int paramIndex)
        {
            var selectFullNameSql = $"{Environment.NewLine}SELECT FullName{Environment.NewLine}FROM Singers{Environment.NewLine}WHERE  TRUE  AND SingerId = @p{paramIndex}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectFullNameSql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.String, "FullName"),
                },
                new List<object[]>
                {
                    new object[] { 1L, fullName },
                }
            ));
            return selectFullNameSql;
        }
    }
}
