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
            var sql = "SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture\r\nFROM Singers AS s\r\nWHERE s.SingerId = @__p_0\r\nLIMIT 1";
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
            var sql = AddFindSingerResult();

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
            var insertSql = "INSERT INTO Singers (SingerId, BirthDate, FirstName, LastName, Picture)\r\nVALUES (@p0, @p1, @p2, @p3, @p4)";
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
            var updateSql = "UPDATE Singers SET LastName = @p0\r\nWHERE SingerId = @p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));
            var selectSingerSql = AddFindSingerResult();
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
            var deleteSql = "DELETE FROM Singers\r\nWHERE SingerId = @p0";
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
            var sql = AddFindSingerResult();
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
            var sql = AddFindSingerResult();
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
            var rawSql = @"INSERT INTO TableWithAllColumnTypes 
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
            var insertSql = "INSERT INTO SingersWithVersion (SingerId, FirstName, LastName, Version)\r\nVALUES (@p0, @p1, @p2, @p3)";
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
            var updateSql = "UPDATE SingersWithVersion SET LastName = @p0, Version = @p1\r\nWHERE SingerId = @p2 AND Version = @p3";
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
            var updateSql = "UPDATE SingersWithVersion SET LastName = @p0, Version = @p1\r\nWHERE SingerId = @p2 AND Version = @p3";
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
            var insertSql = "INSERT INTO Venues (Code, Active, Capacity, Name, Ratings)\r\nVALUES (@p0, @p1, @p2, @p3, @p4)";
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
            var insertSql = "INSERT INTO Venues (Code, Active, Capacity, Name, Ratings)\r\nVALUES (@p0, @p1, @p2, @p3, @p4)";
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
            var sql = "SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture\r\nFROM Singers AS s\r\nORDER BY s.LastName\r\nLIMIT @__p_0";
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
            var sql = "SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture\r\nFROM Singers AS s\r\nORDER BY s.LastName\r\nLIMIT @__p_1 OFFSET @__p_0";
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
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture\r\nFROM Singers AS s\r\nORDER BY s.LastName\r\nLIMIT {long.MaxValue / 2} OFFSET @__p_0";
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
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture, a.AlbumId, a.ReleaseDate, a.SingerId, a.Title\r\nFROM Singers AS s\r\nINNER JOIN Albums AS a ON s.SingerId = a.SingerId";
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
            var sql = "SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture, a.AlbumId, a.ReleaseDate, a.SingerId, a.Title\r\nFROM Singers AS s\r\nLEFT JOIN Albums AS a ON s.SingerId = a.SingerId";
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
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture\r\nFROM Singers AS s\r\nWHERE STRPOS(s.FirstName, @__firstName_0) > 0";
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
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture\r\nFROM Singers AS s\r\nWHERE (@__fullName_0 = '') OR STARTS_WITH(s.FullName, @__fullName_0)";
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
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture\r\nFROM Singers AS s\r\nWHERE (@__fullName_0 = '') OR ENDS_WITH(s.FullName, @__fullName_0)";
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
            var sql = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture\r\nFROM Singers AS s\r\nWHERE CHAR_LENGTH(s.FirstName) > @__minLength_0";
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
            var sql = "SELECT REGEXP_REPLACE(s.FirstName, @__regex_1, @__replacement_2)\r\nFROM Singers AS s\r\nWHERE s.SingerId = @__singerId_0";
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

        private string AddFindSingerResult(string sql = "SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture\r\nFROM Singers AS s\r\nWHERE s.SingerId = @__p_0\r\nLIMIT 1")
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
            var selectFullNameSql = $"\r\nSELECT FullName\r\nFROM Singers\r\nWHERE  TRUE  AND SingerId = @p{paramIndex}";
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
