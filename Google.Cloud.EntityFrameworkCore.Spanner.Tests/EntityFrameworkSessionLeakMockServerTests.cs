// Copyright 2023 Google LLC
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
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Xunit;
using SpannerDate = Google.Cloud.EntityFrameworkCore.Spanner.Storage.SpannerDate;
using V1 = Google.Cloud.Spanner.V1;

#pragma warning disable EF1001
namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
    internal class LimitedSessionsSampleDbContext : SpannerSampleDbContext
    {
        private readonly string _connectionString;
        private readonly SessionPoolManager _manager;

        internal LimitedSessionsSampleDbContext(string connectionString, SessionPoolManager manager)
        {
            _connectionString = connectionString;
            _manager = manager;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (optionsBuilder.IsConfigured)
            {
                return;
            }
            var builder = new SpannerConnectionStringBuilder(_connectionString, ChannelCredentials.Insecure)
            {
                SessionPoolManager = _manager
            };
            optionsBuilder
                .UseSpanner(new SpannerRetriableConnection(new SpannerConnection(builder)), _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                .UseMutations(MutationUsage.Never)
                .UseLazyLoadingProxies();
        }
    }

    /// <summary>
    /// Tests CRUD operations using an in-mem Spanner mock server.
    /// </summary>
    public class EntityFrameworkSessionLeakMockServerTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;
        private readonly SessionPoolManager _manager;

        public EntityFrameworkSessionLeakMockServerTests(SpannerMockServerFixture service)
        {
            _fixture = service;
            service.SpannerMock.Reset();
            
            var options = new SessionPoolOptions
            {
                MinimumPooledSessions = 1,
                MaximumActiveSessions = 2,
                WaitOnResourcesExhausted = ResourcesExhaustedBehavior.Fail
            };
            _manager = SessionPoolManager.Create(options);
        }

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";
        
        private static async Task Repeat(int count, Func<Task> action)
        {
            for (var i = 0; i < count; i++)
            {
                await action();
            }
        }

        private async Task Repeat(Func<Task> action)
        {
            await Repeat(_manager.SessionPoolOptions.MaximumActiveSessions + 1, action);
        }

        /// <summary>
        /// Creates a Entity Framework Database Context with a limited number of sessions and
        /// that is configured to fail if all sessions in the pool have been exhausted.
        /// </summary>
        /// <returns>A DbContext with a limited number of sessions</returns>
        private LimitedSessionsSampleDbContext CreateContext() => new (ConnectionString, _manager);
        
        [Fact]
        public async Task FindSingerAsync_ReturnsNull_IfNotFound()
        {
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.Type, string>>(),
                new List<object[]>()
            ));

            using var db = CreateContext();
            await Repeat(async () =>
            {
                var singer = await db.Singers.FindAsync(1L);
                Assert.Null(singer);
            });
        }

        [Fact]
        public async Task FindSingerAsync_ReturnsInstance_IfFound()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            using var db = CreateContext();
            await Repeat(async () => {
                var singer = await db.Singers.FindAsync(1L);
                Assert.Equal(1L, singer?.SingerId);
            });
        }

        [Fact]
        public async Task InsertSinger_SelectsFullName()
        {
            // Setup results.
            var insertSql = $"INSERT INTO `Singers` (`SingerId`, `BirthDate`, `FirstName`, `LastName`, `Picture`)" +
                $"{Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4){Environment.NewLine}" +
                $"THEN RETURN `FullName`";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type{Code = V1.TypeCode.String}, "FullName", "Alice Morrison"));

            using var db = CreateContext();
            await Repeat(async () =>
            {
                db.Singers.Add(new Singers
                {
                    SingerId = new Random().NextInt64(),
                    FirstName = "Alice",
                    LastName = "Morrison",
                });
                var updateCount = await db.SaveChangesAsync();
                Assert.Equal(1L, updateCount);
            });
        }

        [Fact]
        public async Task UpdateSinger_SelectsFullName()
        {
            // Setup results.
            var updateSql = $"UPDATE `Singers` SET `LastName` = @p0{Environment.NewLine}WHERE `SingerId` = @p1{Environment.NewLine}" +
                            $"THEN RETURN `FullName`";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type{Code = V1.TypeCode.String}, "FullName", "Alice Pieterson-Morrison"));
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, " +
                $"`s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            using var db = CreateContext();
            await Repeat(async () =>
            {
                var singer = await db.Singers.FindAsync(1L);
                singer!.LastName = Guid.NewGuid().ToString();
                var updateCount = await db.SaveChangesAsync();
                Assert.Equal(1L, updateCount);
            });
        }

        [Fact]
        public async Task DeleteSinger_DoesNotSelectFullName()
        {
            // Setup results.
            var deleteSql = $"DELETE FROM `Singers`{Environment.NewLine}WHERE `SingerId` = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(deleteSql, StatementResult.CreateUpdateCount(1L));

            using var db = CreateContext();
            await Repeat(async () =>
            {
                db.Singers.Remove(new Singers { SingerId = 1L });
                var updateCount = await db.SaveChangesAsync();
                Assert.Equal(1L, updateCount);
            });
        }

        [Fact]
        public async Task CanUseReadOnlyTransaction()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");
            using var db = CreateContext();

            await Repeat(async () =>
            {
                using var transaction = await db.Database.BeginReadOnlyTransactionAsync();
                Assert.NotNull(await db.Singers.FindAsync(1L));
                // Read-only transactions must be committed or rolled back to mark their end.
                await transaction.CommitAsync();
            });
        }

        [Fact]
        public async Task CanUseReadOnlyTransactionWithTimestampBound()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");
            using var db = CreateContext();

            await Repeat(async () =>
            {
                using var transaction =
                    await db.Database.BeginReadOnlyTransactionAsync(
                        TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(10)));
                Assert.NotNull(await db.Singers.FindAsync(1L));
                // Read-only transactions must be committed or rolled back to mark their end.
                await transaction.CommitAsync();
            });
        }

        [Fact]
        public async Task CanReadWithMaxStaleness()
        {
            AddFindSingerResult($"-- max_staleness: 10{Environment.NewLine}{Environment.NewLine}" +
                  $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                  $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                  $"FROM `Singers` AS `s`{Environment.NewLine}" +
                  $"WHERE `s`.`SingerId` = @__id_0{Environment.NewLine}LIMIT 1");

            using var db = CreateContext();
            var id = 1L;
            await Repeat(async () =>
            {
                await db.Singers.WithTimestampBound(TimestampBound.OfMaxStaleness(TimeSpan.FromSeconds(10)))
                    .Where(s => s.SingerId == id).FirstAsync();
            });
        }

        [Fact]
        public async Task CanReadWithExactStaleness()
        {
            AddFindSingerResult($"-- exact_staleness: 5{CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator}5{Environment.NewLine}{Environment.NewLine}" +
                  $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                  $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                  $"WHERE `s`.`SingerId` = @__id_0{Environment.NewLine}LIMIT 1");

            using var db = CreateContext();
            var id = 1L;
            await Repeat(async () =>
            {
                await db.Singers.WithTimestampBound(TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(5.5)))
                    .Where(s => s.SingerId == id).FirstAsync();
            });
        }

        [Fact]
        public async Task CanReadWithMinReadTimestamp()
        {
            AddFindSingerResult($"-- min_read_timestamp: 2021-09-08T15:18:01.1230000Z{Environment.NewLine}{Environment.NewLine}" +
                  $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                  $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                  $"WHERE `s`.`SingerId` = @__id_0{Environment.NewLine}LIMIT 1");

            using var db = CreateContext();
            var id = 1L;
            await Repeat(async () =>
            {
                await db.Singers
                    .WithTimestampBound(
                        TimestampBound.OfMinReadTimestamp(DateTime.Parse("2021-09-08T17:18:01.123+02:00")
                            .ToUniversalTime()))
                    .Where(s => s.SingerId == id).FirstAsync();
            });
        }

        [Fact]
        public async Task CanReadWithReadTimestamp()
        {
            AddFindSingerResult($"-- read_timestamp: 2021-09-08T15:18:02.0000000Z{Environment.NewLine}{Environment.NewLine}" +
                  $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                  $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                  $"WHERE `s`.`SingerId` = @__id_0{Environment.NewLine}LIMIT 1");

            using var db = CreateContext();
            var id = 1L;
            await Repeat(async () =>
            {
                await db.Singers
                    .WithTimestampBound(
                        TimestampBound.OfReadTimestamp(DateTime.Parse("2021-09-08T15:18:02Z").ToUniversalTime()))
                    .Where(s => s.SingerId == id).FirstAsync();
            });
        }

        [Fact]
        public async Task InsertUsingRawSqlReturnsUpdateCountWithoutAdditionalSelectCommand()
        {
            var today = SpannerDate.FromDateTime(DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Unspecified));
            var now = DateTime.UtcNow;
            var id = 1L;
            var rawSql = @$"INSERT INTO `TableWithAllColumnTypes` 
                              (`ColBool`, `ColBoolArray`, `ColBytes`, `ColBytesMax`, `ColBytesArray`, `ColBytesMaxArray`,
                               `ColDate`, `ColDateArray`, `ColFloat64`, `ColFloat64Array`, `ColInt64`, `ColInt64Array`,
                               `ColNumeric`, `ColNumericArray`, `ColString`, `ColStringArray`, `ColStringMax`, `ColStringMaxArray`,
                               `ColTimestamp`, `ColTimestampArray`, `ColJson`, `ColJsonArray`)
                              VALUES
                              (@ColBool, @ColBoolArray, @ColBytes, @ColBytesMax, @ColBytesArray, @ColBytesMaxArray,
                               @ColDate, @ColDateArray, @ColFloat64, @ColFloat64Array, @ColInt64, @ColInt64Array,
                               @ColNumeric, @ColNumericArray, @ColString, @ColStringArray, @ColStringMax, @ColStringMaxArray,
                               @ColTimestamp, @ColTimestampArray, @ColJson, @ColJsonArray)";
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
                ColDateArray = new List<DateOnly?> { new SpannerDate(2020, 12, 28), new SpannerDate(2010, 1, 1), today },
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
                ColJson = JsonDocument.Parse("{\"key1\": \"value1\", \"key2\": \"value2\"}"),
                ColJsonArray = new List<JsonDocument>{ JsonDocument.Parse("{\"key1\": \"value1\", \"key2\": \"value2\"}"), JsonDocument.Parse("{\"key1\": \"value3\", \"key2\": \"value4\"}") },
            };
            
            using var db = CreateContext();
            await Repeat(async () =>
            {
                var updateCount = await db.Database.ExecuteSqlRawAsync(rawSql,
                    new SpannerParameter("ColBool", SpannerDbType.Bool, row.ColBool),
                    new SpannerParameter("ColBoolArray", SpannerDbType.ArrayOf(SpannerDbType.Bool), row.ColBoolArray),
                    new SpannerParameter("ColBytes", SpannerDbType.Bytes, row.ColBytes),
                    new SpannerParameter("ColBytesMax", SpannerDbType.Bytes, row.ColBytesMax),
                    new SpannerParameter("ColBytesArray", SpannerDbType.ArrayOf(SpannerDbType.Bytes),
                        row.ColBytesArray),
                    new SpannerParameter("ColBytesMaxArray", SpannerDbType.ArrayOf(SpannerDbType.Bytes),
                        row.ColBytesMaxArray),
                    new SpannerParameter("ColDate", SpannerDbType.Date, row.ColDate),
                    new SpannerParameter("ColDateArray", SpannerDbType.ArrayOf(SpannerDbType.Date), row.ColDateArray),
                    new SpannerParameter("ColFloat64", SpannerDbType.Float64, row.ColFloat64),
                    new SpannerParameter("ColFloat64Array", SpannerDbType.ArrayOf(SpannerDbType.Float64),
                        row.ColFloat64Array),
                    new SpannerParameter("ColInt64", SpannerDbType.Int64, row.ColInt64),
                    new SpannerParameter("ColInt64Array", SpannerDbType.ArrayOf(SpannerDbType.Int64),
                        row.ColInt64Array),
                    new SpannerParameter("ColNumeric", SpannerDbType.Numeric, row.ColNumeric),
                    new SpannerParameter("ColNumericArray", SpannerDbType.ArrayOf(SpannerDbType.Numeric),
                        row.ColNumericArray),
                    new SpannerParameter("ColString", SpannerDbType.String, row.ColString),
                    new SpannerParameter("ColStringArray", SpannerDbType.ArrayOf(SpannerDbType.String),
                        row.ColStringArray),
                    new SpannerParameter("ColStringMax", SpannerDbType.String, row.ColStringMax),
                    new SpannerParameter("ColStringMaxArray", SpannerDbType.ArrayOf(SpannerDbType.String),
                        row.ColStringMaxArray),
                    new SpannerParameter("ColTimestamp", SpannerDbType.Timestamp, row.ColTimestamp),
                    new SpannerParameter("ColTimestampArray", SpannerDbType.ArrayOf(SpannerDbType.Timestamp),
                        row.ColTimestampArray),
                    new SpannerParameter("ColJson", SpannerDbType.Json, row.ColJson?.ToString()),
                    new SpannerParameter("ColJsonArray", SpannerDbType.ArrayOf(SpannerDbType.Json),
                        row.ColJsonArray?.Select(d => d?.ToString()))
                );
                Assert.Equal(1, updateCount);
            });
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task ExplicitAndImplicitTransactionIsRetried(bool disableInternalRetries, bool useExplicitTransaction)
        {
            // Setup results.
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "c", 1));
            var insertSql = $"INSERT INTO `Venues` (`Code`, `Active`, `Capacity`, `Name`, `Ratings`)" +
                            $"{Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

            using var db = CreateContext();
            await Repeat(async () =>
            {
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
                    Code = Guid.NewGuid().ToString(),
                    Name = "Concert Hall",
                });

                // We can only disable internal retries when using explicit transactions. Otherwise internal retries
                // are always used.
                if (disableInternalRetries && useExplicitTransaction)
                {
                    // The transaction must have been initialized for it to fail at all. Otherwise, the client library
                    // will automatically retry the statement.
                    var cmd = transaction.GetDbTransaction().Connection!.CreateCommand();
                    cmd.CommandText = "SELECT 1";
                    cmd.Transaction = transaction.GetDbTransaction();
                    await cmd.ExecuteScalarAsync();
                    // Abort the next statement that is executed on the mock server.
                    _fixture.SpannerMock.AbortNextStatement();
                    var e = await Assert.ThrowsAsync<SpannerException>(() => db.SaveChangesAsync());
                    Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
                }
                else
                {
                    // Abort the next statement that is executed on the mock server.
                    _fixture.SpannerMock.AbortNextStatement();
                    var updateCount = await db.SaveChangesAsync();
                    Assert.Equal(1L, updateCount);
                    if (useExplicitTransaction)
                    {
                        await transaction.CommitAsync();
                    }
                }
                transaction?.Dispose();
            });
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task ExplicitAndImplicitTransactionIsRetried_WhenUsingRawSql(bool disableInternalRetries, bool useExplicitTransaction)
        {
            // Setup results.
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "c", 1));
            var insertSql = $"INSERT INTO `Venues` (`Code`, `Active`, `Capacity`, `Name`, `Ratings`)" +
                            $"{Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

            await using var db = CreateContext();
            await Repeat(async () =>
            {
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
                    // The transaction must have been initialized for it to fail at all. Otherwise, the client library
                    // will automatically retry the statement.
                    var cmd = transaction.GetDbTransaction().Connection!.CreateCommand();
                    cmd.CommandText = "SELECT 1";
                    cmd.Transaction = transaction.GetDbTransaction();
                    await cmd.ExecuteScalarAsync();
                    // Abort the next statement that is executed on the mock server.
                    _fixture.SpannerMock.AbortNextStatement();
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
                    // Abort the next statement that is executed on the mock server.
                    _fixture.SpannerMock.AbortNextStatement();
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
                }
                transaction?.Dispose();
            });
        }

        [Fact]
        public async Task CanUseLimitWithoutOffset()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}ORDER BY `s`.`LastName`" +
                $"{Environment.NewLine}LIMIT @__p_0";
            AddFindSingerResult(sql);

            await Repeat(async () =>
            {
                await db.Singers
                    .OrderBy(s => s.LastName)
                    .Take(1)
                    .ToListAsync();
            });
        }

        [Fact]
        public async Task CanUseLimitWithOffset()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"ORDER BY `s`.`LastName`{Environment.NewLine}LIMIT @__p_1 OFFSET @__p_0";
            AddFindSingerResult(sql);

            await Repeat(async () =>
            {
                await db.Singers
                    .OrderBy(s => s.LastName)
                    .Skip(2)
                    .Take(1)
                    .ToListAsync();
            });
        }

        [Fact]
        public async Task CanUseOffsetWithoutLimit()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"ORDER BY `s`.`LastName`{Environment.NewLine}LIMIT {long.MaxValue / 2} OFFSET @__p_0";
            AddFindSingerResult(sql);

            await Repeat(async () =>
            {
                await db.Singers
                    .OrderBy(s => s.LastName)
                    .Skip(3)
                    .ToListAsync();
            });
        }

        [Fact]
        public async Task CanUseInnerJoin()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`, `a`.`AlbumId`, `a`.`Awards`, `a`.`ReleaseDate`, `a`.`SingerId`, `a`.`Title`{Environment.NewLine}" +
                      $"FROM `Singers` AS `s`{Environment.NewLine}" +
                      $"INNER JOIN `Albums` AS `a` ON `s`.`SingerId` = `a`.`SingerId`";
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
                    Tuple.Create(V1.TypeCode.Array, "Awards"),
                    Tuple.Create(V1.TypeCode.Date, "ReleaseDate"),
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.String, "Title"),
                },
                new List<object[]>
                {
                    new object[] { 1L, null, "Zeke", "Zeke Peterson", "Peterson", null, 100L, new List<string>{"award 1", "award 2"}, null, 1L, "Some Title" },
                }
            ));

            await Repeat(async () =>
            {
                await db.Singers
                    .Join(db.Albums, a => a.SingerId, s => s.SingerId, (s, a) => new { Singer = s, Album = a })
                    .ToListAsync();
            });
        }

        [Fact]
        public async Task CanUseOuterJoin()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`, `a`.`AlbumId`, `a`.`Awards`, `a`.`ReleaseDate`, `a`.`SingerId`, `a`.`Title`{Environment.NewLine}" +
                      $"FROM `Singers` AS `s`{Environment.NewLine}" +
                      $"LEFT JOIN `Albums` AS `a` ON `s`.`SingerId` = `a`.`SingerId`";
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
                    Tuple.Create(V1.TypeCode.Array, "Awards"),
                    Tuple.Create(V1.TypeCode.Date, "ReleaseDate"),
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.String, "Title"),
                },
                new List<object[]>
                {
                    new object[] { 2L, null, "Alice", "Alice Morrison", "Morrison", null, null, null, null, null, null },
                    new object[] { 3L, null, "Zeke", "Zeke Peterson", "Peterson", null, 100L, new List<string>{"award 1", "award 2"}, null, 3L, "Some Title" },
                }
            ));

            await Repeat(async () =>
            {
                await db.Singers
                    .GroupJoin(db.Albums, s => s.SingerId, a => a.SingerId, (s, a) => new { Singer = s, Albums = a })
                    .SelectMany(
                        s => s.Albums.DefaultIfEmpty(),
                        (s, a) => new { s.Singer, Album = a })
                    .ToListAsync();
            });
        }

        [Fact]
        public async Task CanUseStringContains()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE STRPOS(`s`.`FirstName`, @__firstName_0) > 0";
            AddFindSingerResult(sql);

            await Repeat(async () =>
            {
                var firstName = "Alice";
                await db.Singers
                    .Where(s => s.FirstName.Contains(firstName))
                    .ToListAsync();
            });
        }

        [Fact]
        public async Task CanUseStringStartsWith()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE STARTS_WITH(`s`.`FullName`, @__fullName_0)";
            AddFindSingerResult(sql);

            await Repeat(async () => {
                var fullName = "Alice M";
                await db.Singers
                    .Where(s => s.FullName.StartsWith(fullName))
                    .ToListAsync();
            });
        }

        [Fact]
        public async Task CanUseStringEndsWith()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE ENDS_WITH(`s`.`FullName`, @__fullName_0)";
            AddFindSingerResult(sql);

            await Repeat(async () =>
            {
                var fullName = " Morrison";
                await db.Singers
                    .Where(s => s.FullName.EndsWith(fullName))
                    .ToListAsync();
            });
        }

        [Fact]
        public async Task CanUseStringLength()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE CHAR_LENGTH(`s`.`FirstName`) > @__minLength_0";
            AddFindSingerResult(sql);

            await Repeat(async () =>
            {
                var minLength = 4;
                await db.Singers
                    .Where(s => s.FirstName.Length > minLength)
                    .ToListAsync();
            });
        }

        [Fact]
        public async Task CanUseStringConcat()
        {
            using var db = CreateContext();
            var sql = $"SELECT CONCAT(`s`.`FirstName`, ''' ''', `s`.`LastName`, CAST(`s`.`SingerId` AS STRING)){Environment.NewLine}" + 
                      $"FROM `Singers` AS `s`{Environment.NewLine}" + 
                      "LIMIT 1";
            AddFindSingerResult(sql);

            await Repeat(async () =>
            {
                await db.Singers
                    .Select(s => string.Concat(s.FirstName, " ", s.LastName, s.SingerId.ToString()))
                    .FirstOrDefaultAsync();
            });
        }

        [Fact]
        public async Task CanUseStringPlus()
        {
            using var db = CreateContext();
            var sql = $"SELECT (COALESCE(`s`.`FirstName`, '''''')||''' ''')||`s`.`LastName`{Environment.NewLine}" + 
                      $"FROM `Singers` AS `s`{Environment.NewLine}" + 
                      "LIMIT 1";
            AddFindSingerResult(sql);

            await Repeat(async () =>
            {
                await db.Singers
                    .Select(s => s.FirstName + " " + s.LastName)
                    .FirstOrDefaultAsync();
            });
        }

        [Fact]
        public async Task CanUseRegexReplace()
        {
            using var db = CreateContext();
            var sql = $"SELECT REGEXP_REPLACE(`s`.`FirstName`, @__regex_1, @__replacement_2)" +
                $"{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}WHERE `s`.`SingerId` = @__singerId_0";
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
            await Repeat(async () =>
            {
                var firstNames = await db.Singers
                    .Where(s => s.SingerId == singerId)
                    .Select(s => regex.Replace(s.FirstName, replacement))
                    .ToListAsync();
                Assert.Collection(firstNames, s => Assert.Equal("Allison", s));
            });
        }

        [Fact]
        public async Task CanUseDateTimeAddYears()
        {
            using var db = CreateContext();
            // Note: AddYears cannot be applied server side to a TIMESTAMP, only to a DATE, so this is handled client side.
            var sql = $"SELECT `c`.`StartTime`{Environment.NewLine}FROM `Concerts` AS `c`" +
                $"{Environment.NewLine}WHERE `c`.`SingerId` = @__singerId_0";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Concerts
                    .Where(c => c.SingerId == singerId)
                    .Select(s => s.StartTime.AddYears(1))
                    .ToListAsync();
                Assert.Collection(startTimes,
                    s => Assert.Equal(new DateTime(2022, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
            });
        }

        [Fact]
        public async Task CanUseSpannerDateAddYears()
        {
            using var db = CreateContext();
            var sql = $"SELECT DATE_ADD(`s`.`BirthDate`, INTERVAL 1 YEAR){Environment.NewLine}" +
                $"FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__singerId_0 AND `s`.`BirthDate` IS NOT NULL";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Singers
                    .Where(s => s.SingerId == singerId && s.BirthDate != null)
                    .Select(s => s.BirthDate.Value.AddYears(1))
                    .ToListAsync();
                Assert.Collection(startTimes, s => Assert.Equal(new SpannerDate(1980, 1, 20), s));
            });
        }

        [Fact]
        public async Task CanUseDateTimeAddMonths()
        {
            using var db = CreateContext();
            // Note: AddMonths cannot be applied server side to a TIMESTAMP, only to a DATE, so this is handled client side.
            var sql = $"SELECT `c`.`StartTime`{Environment.NewLine}FROM `Concerts` AS `c`" +
                $"{Environment.NewLine}WHERE `c`.`SingerId` = @__singerId_0";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Concerts
                    .Where(c => c.SingerId == singerId)
                    .Select(s => s.StartTime.AddMonths(1))
                    .ToListAsync();
                Assert.Collection(startTimes,
                    s => Assert.Equal(new DateTime(2021, 2, 20, 18, 0, 0, DateTimeKind.Utc), s));
            });
        }

        [Fact]
        public async Task CanUseSpannerDateAddMonths()
        {
            using var db = CreateContext();
            var sql = $"SELECT DATE_ADD(`s`.`BirthDate`, INTERVAL 1 MONTH){Environment.NewLine}" +
                $"FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__singerId_0 AND `s`.`BirthDate` IS NOT NULL";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Singers
                    .Where(s => s.SingerId == singerId && s.BirthDate != null)
                    .Select(s => s.BirthDate.Value.AddMonths(1))
                    .ToListAsync();
                Assert.Collection(startTimes, s => Assert.Equal(new SpannerDate(1980, 1, 20), s));
            });
        }

        [Fact]
        public async Task CanUseDateTimeAddDays()
        {
            using var db = CreateContext();
            var sql = $"SELECT TIMESTAMP_ADD(`c`.`StartTime`, INTERVAL CAST(1.0 AS INT64) DAY)" +
                $"{Environment.NewLine}FROM `Concerts` AS `c`{Environment.NewLine}" +
                $"WHERE `c`.`SingerId` = @__singerId_0";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Concerts
                    .Where(c => c.SingerId == singerId)
                    .Select(s => s.StartTime.AddDays(1))
                    .ToListAsync();
                Assert.Collection(startTimes,
                    s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
            });
        }

        [Fact]
        public async Task CanUseSpannerDateAddDays()
        {
            using var db = CreateContext();
            var sql = $"SELECT DATE_ADD(`s`.`BirthDate`, INTERVAL 1 DAY){Environment.NewLine}" +
                $"FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__singerId_0 AND `s`.`BirthDate` IS NOT NULL";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Singers
                    .Where(s => s.SingerId == singerId && s.BirthDate != null)
                    .Select(s => s.BirthDate.Value.AddDays(1))
                    .ToListAsync();
                Assert.Collection(startTimes, s => Assert.Equal(new SpannerDate(1980, 1, 20), s));
            });
        }

        [Fact]
        public async Task CanUseDateTimeAddHours()
        {
            using var db = CreateContext();
            var sql = $"SELECT TIMESTAMP_ADD(`c`.`StartTime`, INTERVAL CAST(1.0 AS INT64) HOUR)" +
                $"{Environment.NewLine}FROM `Concerts` AS `c`{Environment.NewLine}" +
                $"WHERE `c`.`SingerId` = @__singerId_0";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Concerts
                    .Where(c => c.SingerId == singerId)
                    .Select(s => s.StartTime.AddHours(1))
                    .ToListAsync();
                Assert.Collection(startTimes,
                    s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
            });
        }

        [Fact]
        public async Task CanUseDateTimeAddMinutes()
        {
            using var db = CreateContext();
            var sql = $"SELECT TIMESTAMP_ADD(`c`.`StartTime`, INTERVAL CAST(1.0 AS INT64) MINUTE)" +
                $"{Environment.NewLine}FROM `Concerts` AS `c`{Environment.NewLine}" +
                $"WHERE `c`.`SingerId` = @__singerId_0";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Concerts
                    .Where(c => c.SingerId == singerId)
                    .Select(s => s.StartTime.AddMinutes(1))
                    .ToListAsync();
                Assert.Collection(startTimes,
                    s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
            });
        }

        [Fact]
        public async Task CanUseDateTimeAddSeconds()
        {
            using var db = CreateContext();
            var sql = $"SELECT TIMESTAMP_ADD(`c`.`StartTime`, INTERVAL CAST(1.0 AS INT64) SECOND)" +
                $"{Environment.NewLine}FROM `Concerts` AS `c`{Environment.NewLine}" +
                $"WHERE `c`.`SingerId` = @__singerId_0";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Concerts
                    .Where(c => c.SingerId == singerId)
                    .Select(s => s.StartTime.AddSeconds(1))
                    .ToListAsync();
                Assert.Collection(startTimes,
                    s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
            });
        }

        [Fact]
        public async Task CanUseDateTimeAddMilliseconds()
        {
            using var db = CreateContext();
            var sql = $"SELECT TIMESTAMP_ADD(`c`.`StartTime`, INTERVAL CAST(1.0 AS INT64) MILLISECOND)" +
                $"{Environment.NewLine}FROM `Concerts` AS `c`{Environment.NewLine}" +
                $"WHERE `c`.`SingerId` = @__singerId_0";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Concerts
                    .Where(c => c.SingerId == singerId)
                    .Select(s => s.StartTime.AddMilliseconds(1))
                    .ToListAsync();
                Assert.Collection(startTimes,
                    s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
            });
        }

        [Fact]
        public async Task CanUseDateTimeAddTicks()
        {
            using var db = CreateContext();
            var sql = $"SELECT TIMESTAMP_ADD(`c`.`StartTime`, INTERVAL 100 * 1 NANOSECOND)" +
                $"{Environment.NewLine}FROM `Concerts` AS `c`{Environment.NewLine}" +
                $"WHERE `c`.`SingerId` = @__singerId_0";
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
            await Repeat(async () =>
            {
                var startTimes = await db.Concerts
                    .Where(c => c.SingerId == singerId)
                    .Select(s => s.StartTime.AddTicks(1))
                    .ToListAsync();
                Assert.Collection(startTimes,
                    s => Assert.Equal(new DateTime(2021, 1, 20, 18, 0, 0, DateTimeKind.Utc), s));
            });
        }

        [Fact]
        public async Task CanUseLongAbs()
        {
            using var db = CreateContext();
            var sql = $"SELECT ABS(`t`.`ColInt64`){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var absId = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Abs(t.ColInt64))
                    .FirstOrDefaultAsync();
                Assert.Equal(1L, absId);
            });
        }

        [Fact]
        public async Task CanUseDoubleAbs()
        {
            using var db = CreateContext();
            var sql = $"SELECT ABS(COALESCE(`t`.`ColFloat64`, 0.0)){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var absId = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Abs(t.ColFloat64.GetValueOrDefault()))
                    .FirstOrDefaultAsync();
                Assert.Equal(3.14d, absId);
            });
        }

        [Fact]
        public async Task CanUseDecimalAbs()
        {
            using var db = CreateContext();
            var sql = $"SELECT ABS(COALESCE(`t`.`ColNumeric`, 0)){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var absId = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Abs(t.ColNumeric.GetValueOrDefault().ToDecimal(LossOfPrecisionHandling.Truncate)))
                    .FirstOrDefaultAsync();
                Assert.Equal(3.14m, absId);
            });
        }

        [Fact]
        public async Task CanUseLongMax()
        {
            using var db = CreateContext();
            var sql = $"SELECT GREATEST(`t`.`ColInt64`, 2){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var max = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Max(t.ColInt64, 2L))
                    .FirstOrDefaultAsync();
                Assert.Equal(2L, max);
            });
        }

        [Fact]
        public async Task CanUseDoubleMax()
        {
            using var db = CreateContext();
            var sql = $"SELECT GREATEST(COALESCE(`t`.`ColFloat64`, 0.0), 3.1400000000000001)" +
                $"{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`{Environment.NewLine}" +
                $"WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var max = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Max(t.ColFloat64.GetValueOrDefault(), 3.14d))
                    .FirstOrDefaultAsync();
                Assert.Equal(3.14d, max);
            });
        }

        [Fact]
        public async Task CanUseLongMin()
        {
            using var db = CreateContext();
            var sql = $"SELECT LEAST(`t`.`ColInt64`, 2){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var min = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Min(t.ColInt64, 2L))
                    .FirstOrDefaultAsync();
                Assert.Equal(1L, min);
            });
        }

        [Fact]
        public async Task CanUseDoubleMin()
        {
            using var db = CreateContext();
            var sql = $"SELECT LEAST(COALESCE(`t`.`ColFloat64`, 0.0), 3.1400000000000001)" +
                $"{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`{Environment.NewLine}" +
                $"WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var min = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Min(t.ColFloat64.GetValueOrDefault(), 3.14d))
                    .FirstOrDefaultAsync();
                Assert.Equal(0.1d, min);
            });
        }

        [Fact]
        public async Task CanUseRound()
        {
            using var db = CreateContext();
            var sql = $"SELECT ROUND(COALESCE(`t`.`ColFloat64`, 0.0)){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var rounded = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Round(t.ColFloat64.GetValueOrDefault(), MidpointRounding.AwayFromZero))
                    .FirstOrDefaultAsync();
                Assert.Equal(3.0d, rounded);
            });
        }

        [Fact]
        public async Task CanUseDecimalRound()
        {
            using var db = CreateContext();
            var sql = $"SELECT ROUND(COALESCE(`t`.`ColNumeric`, 0)){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var rounded = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t =>
                        Math.Round(t.ColNumeric.GetValueOrDefault().ToDecimal(LossOfPrecisionHandling.Truncate),
                            MidpointRounding.AwayFromZero))
                    .FirstOrDefaultAsync();
                Assert.Equal(3.0m, rounded);
            });
        }

        [Fact]
        public async Task CanUseRoundWithDigits()
        {
            using var db = CreateContext();
            var sql = $"SELECT ROUND(COALESCE(`t`.`ColFloat64`, 0.0), 1){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var rounded = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Round(t.ColFloat64.GetValueOrDefault(), 1, MidpointRounding.AwayFromZero))
                    .FirstOrDefaultAsync();
                Assert.Equal(3.1d, rounded);
            });
        }

        [Fact]
        public async Task CanUseCeiling()
        {
            using var db = CreateContext();
            var sql = $"SELECT CEIL(COALESCE(`t`.`ColFloat64`, 0.0)){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var ceil = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Ceiling(t.ColFloat64.GetValueOrDefault()))
                    .FirstOrDefaultAsync();
                Assert.Equal(4.0d, ceil);
            });
        }

        [Fact]
        public async Task CanUseFloor()
        {
            using var db = CreateContext();
            var sql = $"SELECT FLOOR(COALESCE(`t`.`ColFloat64`, 0.0)){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var floor = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => Math.Floor(t.ColFloat64.GetValueOrDefault()))
                    .FirstOrDefaultAsync();
                Assert.Equal(3.0d, floor);
            });
        }

        [Fact]
        public async Task CanUseDateTimeProperties()
        {
            using var db = CreateContext();
            var sql = $"SELECT EXTRACT(YEAR FROM COALESCE(`t`.`ColTimestamp`, TIMESTAMP '0001-01-01T00:00:00Z') AT TIME ZONE 'UTC') AS `Year`, " +
                $"EXTRACT(MONTH FROM COALESCE(`t`.`ColTimestamp`, TIMESTAMP '0001-01-01T00:00:00Z') AT TIME ZONE 'UTC') AS `Month`" +
                $"{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var extracted = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => new
                    {
                        t.ColTimestamp.GetValueOrDefault().Year,
                        t.ColTimestamp.GetValueOrDefault().Month,
                    })
                    .FirstOrDefaultAsync();
                Assert.Equal(2021, extracted?.Year);
                Assert.Equal(1, extracted?.Month);
            });
        }

        [Fact]
        public async Task CanUseSpannerDateProperties()
        {
            using var db = CreateContext();
            var sql = $"SELECT EXTRACT(YEAR FROM COALESCE(`t`.`ColDate`, DATE '0001-01-01')) AS `Year`, " +
                "EXTRACT(MONTH FROM COALESCE(`t`.`ColDate`, DATE '0001-01-01')) AS `Month`, " +
                "EXTRACT(DAY FROM COALESCE(`t`.`ColDate`, DATE '0001-01-01')) AS `Day`, " +
                "EXTRACT(DAYOFYEAR FROM COALESCE(`t`.`ColDate`, DATE '0001-01-01')) AS `DayOfYear`, " +
                "EXTRACT(DAYOFWEEK FROM COALESCE(`t`.`ColDate`, DATE '0001-01-01')) - 1 AS `DayOfWeek`" +
                $"{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`{Environment.NewLine}" +
                $"WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
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
                Assert.Equal(2021, extracted?.Year);
                Assert.Equal(1, extracted?.Month);
                Assert.Equal(25, extracted?.Day);
                Assert.Equal(25, extracted?.DayOfYear);
                Assert.Equal(DayOfWeek.Monday, extracted?.DayOfWeek);
            });
        }

        [Fact]
        public async Task CanUseBoolToString()
        {
            using var db = CreateContext();
            var sql = $"SELECT CAST(COALESCE(`t`.`ColBool`, false) AS STRING)" +
                $"{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`{Environment.NewLine}" +
                $"WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var converted = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => t.ColBool.GetValueOrDefault().ToString())
                    .FirstOrDefaultAsync();
                Assert.Equal("TRUE", converted);
            });
        }

        [Fact]
        public async Task CanUseBytesToString()
        {
            using var db = CreateContext();
            var sql = $"SELECT CAST(`t`.`ColBytes` AS STRING){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var converted = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => t.ColBytes.ToString())
                    .FirstOrDefaultAsync();
                Assert.Equal("some bytes", converted);
            });
        }

        [Fact]
        public async Task CanUseLongToString()
        {
            using var db = CreateContext();
            var sql = $"SELECT CAST(`t`.`ColInt64` AS STRING){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var converted = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => t.ColInt64.ToString())
                    .FirstOrDefaultAsync();
                Assert.Equal("100", converted);
            });
        }

        [Fact]
        public async Task CanUseSpannerNumericToString()
        {
            using var db = CreateContext();
            var sql = $"SELECT CAST(COALESCE(`t`.`ColNumeric`, 0) AS STRING){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var converted = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    .Select(t => t.ColNumeric.GetValueOrDefault().ToString())
                    .FirstOrDefaultAsync();
                Assert.Equal("3.14", converted);
            });
        }

        [Fact]
        public async Task CanUseDoubleToString()
        {
            using var db = CreateContext();
            var sql = $"SELECT CAST(COALESCE(`t`.`ColFloat64`, 0.0) AS STRING){Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var converted = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    // ReSharper disable once SpecifyACultureInStringConversionExplicitly
                    .Select(t => t.ColFloat64.GetValueOrDefault().ToString())
                    .FirstOrDefaultAsync();
                Assert.Equal("3.0", converted);
            });
        }

        [Fact]
        public async Task CanUseSpannerDateToString()
        {
            using var db = CreateContext();
            var sql = $"SELECT CAST(COALESCE(`t`.`ColDate`, DATE '0001-01-01') AS STRING)" +
                $"{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0" +
                $"{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var converted = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    // ReSharper disable once SpecifyACultureInStringConversionExplicitly
                    .Select(t => t.ColDate.GetValueOrDefault().ToString())
                    .FirstOrDefaultAsync();
                Assert.Equal("2021-01-25", converted);
            });
        }

        [Fact]
        public async Task CanUseDateTimeToString()
        {
            using var db = CreateContext();
            var sql = $"SELECT FORMAT_TIMESTAMP('''%FT%H:%M:%E*SZ''', COALESCE(`t`.`ColTimestamp`, TIMESTAMP '0001-01-01T00:00:00Z'), '''UTC''')" +
                $"{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`{Environment.NewLine}WHERE `t`.`ColInt64` = @__id_0" +
                $"{Environment.NewLine}LIMIT 1";
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
            await Repeat(async () =>
            {
                var converted = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64 == id)
                    // ReSharper disable once SpecifyACultureInStringConversionExplicitly
                    .Select(t => t.ColTimestamp.GetValueOrDefault().ToString())
                    .FirstOrDefaultAsync();
                Assert.Equal("2021-01-25T12:46:01.982784Z", converted);
            });
        }

        [Fact]
        public async Task CanUseLongListCount()
        {
            using var db = CreateContext();
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColInt64Array`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            await Repeat(async () =>
            {
                var id = await db.TableWithAllColumnTypes
                    .Where(t => t.ColInt64Array.Count == 2)
                    .Select(t => t.ColInt64)
                    .FirstOrDefaultAsync();
                Assert.Equal(1L, id);
            });
        }

        [Fact]
        public async Task CanUseDoubleListCount()
        {
            using var db = CreateContext();
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColFloat64Array`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            await Repeat(async () =>
            {
                var id = await db.TableWithAllColumnTypes
                    .Where(t => t.ColFloat64Array.Count == 2)
                    .Select(t => t.ColInt64)
                    .FirstOrDefaultAsync();
                Assert.Equal(1L, id);
            });
        }

        [Fact]
        public async Task CanUseSpannerNumericListCount()
        {
            using var db = CreateContext();
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColNumericArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            await Repeat(async () =>
            {
                var id = await db.TableWithAllColumnTypes
                    .Where(t => t.ColNumericArray.Count == 2)
                    .Select(t => t.ColInt64)
                    .FirstOrDefaultAsync();
                Assert.Equal(1L, id);
            });
        }

        [Fact]
        public async Task CanUseBoolListCount()
        {
            using var db = CreateContext();
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColBoolArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            await Repeat(async () =>
            {
                var id = await db.TableWithAllColumnTypes
                    .Where(t => t.ColBoolArray.Count == 2)
                    .Select(t => t.ColInt64)
                    .FirstOrDefaultAsync();
                Assert.Equal(1L, id);
            });
        }

        [Fact]
        public async Task CanUseStringListCount()
        {
            using var db = CreateContext();
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColStringArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            await Repeat(async () =>
            {
                var id = await db.TableWithAllColumnTypes
                    .Where(t => t.ColStringArray.Count == 2)
                    .Select(t => t.ColInt64)
                    .FirstOrDefaultAsync();
                Assert.Equal(1L, id);
            });
        }

        [Fact]
        public async Task CanUseSByteArrayListCount()
        {
            using var db = CreateContext();
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColBytesArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            await Repeat(async () =>
            {
                var id = await db.TableWithAllColumnTypes
                    .Where(t => t.ColBytesArray.Count == 2)
                    .Select(t => t.ColInt64)
                    .FirstOrDefaultAsync();
                Assert.Equal(1L, id);
            });
        }

        [Fact]
        public async Task CanUseSpannerDateListCount()
        {
            using var db = CreateContext();
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColDateArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            await Repeat(async () =>
            {
                var id = await db.TableWithAllColumnTypes
                    .Where(t => t.ColDateArray.Count == 2)
                    .Select(t => t.ColInt64)
                    .FirstOrDefaultAsync();
                Assert.Equal(1L, id);
            });
        }

        [Fact]
        public async Task CanUseDateTimeListCount()
        {
            using var db = CreateContext();
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColTimestampArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            await Repeat(async () =>
            {
                var id = await db.TableWithAllColumnTypes
                    .Where(t => t.ColTimestampArray.Count == 2)
                    .Select(t => t.ColInt64)
                    .FirstOrDefaultAsync();
                Assert.Equal(1L, id);
            });
        }

        [Fact]
        public async Task CanInsertCommitTimestamp()
        {
            using var db = CreateContext();
            var sql = "INSERT INTO `TableWithAllColumnTypes` (`ColCommitTS`, `ColInt64`, `ASC`, `ColBool`, " +
                "`ColBoolArray`, `ColBytes`, `ColBytesArray`, `ColBytesMax`, `ColBytesMaxArray`, `ColDate`, `ColDateArray`," +
                " `ColFloat32`, `ColFloat32Array`, `ColFloat64`, `ColFloat64Array`, `ColInt64Array`, `ColJson`, `ColJsonArray`, `ColNumeric`, `ColNumericArray`, `ColString`, " +
                "`ColStringArray`, `ColStringMax`, `ColStringMaxArray`, `ColTimestamp`, `ColTimestampArray`)" +
                $"{Environment.NewLine}VALUES (PENDING_COMMIT_TIMESTAMP(), " +
                $"@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23, @p24){Environment.NewLine}" +
                $"THEN RETURN `ColComputed`";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type{Code = V1.TypeCode.String}, "ColComputed", "Test"));

            await Repeat(async () =>
            {
                db.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes { ColInt64 = new Random().NextInt64() });
                await db.SaveChangesAsync();
            });
        }

        [Fact]
        public async Task CanUpdateCommitTimestamp()
        {
            using var db = CreateContext();
            var sql = $"UPDATE `TableWithAllColumnTypes` SET `ColCommitTS` = PENDING_COMMIT_TIMESTAMP(), `ColBool` = @p0" +
                $"{Environment.NewLine}WHERE `ColInt64` = @p1{Environment.NewLine}" +
                $"THEN RETURN `ColComputed`";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type{Code = V1.TypeCode.String}, "ColComputed", "Test"));

            await Repeat(async () =>
            {
                var row = new TableWithAllColumnTypes { ColInt64 = new Random().NextInt64() };
                db.TableWithAllColumnTypes.Attach(row);
                row.ColBool = true;
                await db.SaveChangesAsync();
            });
        }

        [Fact]
        public async Task CanUseAsAsyncEnumerable()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`" +
                $"{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}WHERE STRPOS(`s`.`FirstName`, @__firstName_0) > 0";
            AddFindSingerResult(sql);

            await Repeat(async () =>
            {
                var firstName = "Alice";
                var singers = db.Singers
                    .Where(s => s.FirstName.Contains(firstName))
                    .AsAsyncEnumerable();
                await foreach (var singer in singers)
                {
                    Assert.Equal("Morrison", singer.LastName);
                }
            });
        }

        [Fact]
        public async Task CanInsertRowWithCommitTimestampAndComputedColumn()
        {
            using var db = CreateContext();
            var sql = "INSERT INTO `TableWithAllColumnTypes` (`ColCommitTS`, `ColInt64`, `ASC`, `ColBool`, `ColBoolArray`," +
                " `ColBytes`, `ColBytesArray`, `ColBytesMax`, `ColBytesMaxArray`, `ColDate`, `ColDateArray`, `ColFloat32`, `ColFloat32Array`," +
                " `ColFloat64`, `ColFloat64Array`, `ColInt64Array`, `ColJson`, `ColJsonArray`, `ColNumeric`, `ColNumericArray`, `ColString`, `ColStringArray`," +
                $" `ColStringMax`, `ColStringMaxArray`, `ColTimestamp`, `ColTimestampArray`){Environment.NewLine}" +
                $"VALUES (PENDING_COMMIT_TIMESTAMP(), @p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23, @p24){Environment.NewLine}" +
                $"THEN RETURN `ColComputed`";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type{Code = V1.TypeCode.String}, "ColComputed", "Test"));

            await Repeat(async () =>
            {
                db.TableWithAllColumnTypes.Add(
                    new TableWithAllColumnTypes { ColInt64 = new Random().NextInt64() }
                );
                await db.SaveChangesAsync();
            });
        }

        [Fact]
        public async Task CanInsertAllTypes()
        {
            using var db = CreateContext();
            var sql = "INSERT INTO `TableWithAllColumnTypes` (`ColCommitTS`, `ColInt64`, `ASC`, `ColBool`, " +
                      "`ColBoolArray`, `ColBytes`, `ColBytesArray`, `ColBytesMax`, `ColBytesMaxArray`, `ColDate`, `ColDateArray`," +
                      " `ColFloat32`, `ColFloat32Array`, `ColFloat64`, `ColFloat64Array`, `ColInt64Array`, `ColJson`, `ColJsonArray`, `ColNumeric`, `ColNumericArray`, `ColString`, " +
                      "`ColStringArray`, `ColStringMax`, `ColStringMaxArray`, `ColTimestamp`, `ColTimestampArray`)" +
                      $"{Environment.NewLine}VALUES (PENDING_COMMIT_TIMESTAMP(), " +
                      $"@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23, @p24){Environment.NewLine}" +
                      $"THEN RETURN `ColComputed`";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type {Code = V1.TypeCode.String}, "ColComputed", "Test"));

            await Repeat(async () =>
            {
                db.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes
                {
                    ColInt64 = new Random().NextInt64(),
                    ColBool = true,
                    ColBytes = new byte[] { 1, 2, 3 },
                    ColDate = new SpannerDate(2000, 1, 1),
                    ColFloat32 = 3.14f,
                    ColFloat64 = 3.14,
                    ColJson = JsonDocument.Parse("{\"key\": \"value\"}"),
                    ColNumeric = SpannerNumeric.Parse("6.626"),
                    ColString = "test",
                    ColTimestamp = new DateTime(2000, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc),
                    ColBoolArray = new List<bool?> { true, null, false },
                    ColBytesArray = new List<byte[]> { new byte[] { 1, 2, 3 }, null, new byte[] { 3, 2, 1 } },
                    ColBytesMax = new byte[] { },
                    ColDateArray = new List<DateOnly?>
                        { new SpannerDate(2021, 8, 26), null, new SpannerDate(2000, 1, 1) },
                    ColFloat32Array = new List<float?> { 3.14f, null, 6.626f },
                    ColFloat64Array = new List<double?> { 3.14, null, 6.626 },
                    ColInt64Array = new List<long?> { 1, null, 2 },
                    ColJsonArray = new List<JsonDocument>
                    {
                        JsonDocument.Parse("{\"key1\": \"value1\"}"), null, JsonDocument.Parse("{\"key2\": \"value2\"}")
                    },
                    ColNumericArray = new List<SpannerNumeric?>
                        { SpannerNumeric.Parse("3.14"), null, SpannerNumeric.Parse("6.626") },
                    ColStringArray = new List<string> { "test1", null, "test2" },
                    ColStringMax = "",
                    ColTimestampArray = new List<DateTime?>
                    {
                        new DateTime(2000, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc), null,
                        new DateTime(2000, 1, 1, 0, 0, 0, 2, DateTimeKind.Utc)
                    },
                    ColBytesMaxArray = new List<byte[]>(),
                    ColStringMaxArray = new List<string>(),
                });
                await db.SaveChangesAsync();
            });
        }

        [Fact]
        public async Task ReadingInReadWriteTransaction_DoesNotLeakSession()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            await Repeat(async () =>
            {
                using var db = CreateContext();
                using var transaction = await db.Database.BeginTransactionAsync();
                Assert.NotNull(await db.Singers.FindAsync(1L));
                await transaction.CommitAsync();
            });
        }

        [Fact]
        public async Task AbortedReadWriteTransaction_DoesNotLeakSession()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            await Repeat(async () =>
            {
                await using var db = CreateContext();
                await using var transaction = await db.Database.BeginTransactionAsync();
                Assert.NotNull(await db.Singers.FindAsync(1L));
                _fixture.SpannerMock.AbortNextStatement();
                Assert.NotNull(await db.Singers.FindAsync(2L));
                await transaction.CommitAsync();
            });
        }

        [Fact]
        public async Task ReadingInReadWriteTransactionFromSameDbContext_DoesNotLeakSession()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            await using var db = CreateContext();
            await Repeat(async () =>
            {
                await using var transaction = await db.Database.BeginTransactionAsync();
                Assert.NotNull(await db.Singers.FindAsync(1L));
                await transaction.CommitAsync();
            });
        }

        [Fact]
        public async Task MultipleReadWriteTransactionsWithUsingBlocks_DoesNotHoldOnToSessionsForTooLong()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            await using var db = CreateContext();

            await using (var transaction1 = await db.Database.BeginTransactionAsync())
            {
                Assert.NotNull(await db.Singers.FindAsync(1L));
                await transaction1.CommitAsync();
            }

            await using (var transaction2 = await db.Database.BeginTransactionAsync())
            {
                Assert.NotNull(await db.Singers.FindAsync(2L));
                await transaction2.CommitAsync();
            }

            // This works, because the two previous transactions were disposed.
            await using var transaction3 = await db.Database.BeginTransactionAsync();
            Assert.NotNull(await db.Singers.FindAsync(2L));
            await transaction3.CommitAsync();
        }

        [Fact]
        public async Task NestedTransactionsStartNewTransactions()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            using var db1 = CreateContext();
            using (var transaction1 = await db1.Database.BeginTransactionAsync())
            {
                Assert.NotNull(await db1.Singers.FindAsync(1L));
                using var db2 = CreateContext();
                using (var transaction2 = await db2.Database.BeginTransactionAsync())
                {
                    Assert.NotNull(await db2.Singers.FindAsync(2L));
                    
                    // This will now fail because the 2 sessions that the pool is allowed to hold are
                    // still checked out by the above transactions.
                    using var db3 = CreateContext();
                    var exception = await Assert.ThrowsAsync<SpannerException>(() => db3.Database.BeginTransactionAsync());
                    Assert.Equal(ErrorCode.ResourceExhausted, exception.ErrorCode);
                    
                    await transaction2.CommitAsync();
                }
                await transaction1.CommitAsync();
            }
        }

        [Fact]
        public async Task ReadingInReadWriteTransactionWithoutCommitting_DoesNotLeakSession()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            await Repeat(async () =>
            {
                using var db = CreateContext();
                using var transaction = await db.Database.BeginTransactionAsync();
                Assert.NotNull(await db.Singers.FindAsync(1L));
                // NOTE: No Commit or Rollback.
            });
        }

        [Fact]
        public async Task ReadingInReadWriteTransactionFromSameDbContextWithoutCommitting_DoesNotLeakSession()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            using var db = CreateContext();
            await Repeat(async () =>
            {
                using var transaction = await db.Database.BeginTransactionAsync();
                Assert.NotNull(await db.Singers.FindAsync(1L));
                // NOTE: No Commit or Rollback.
            });
        }

        [Fact]
        public async Task DbContextWithoutDisposing_DoesNotLeakSession()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                                $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            await Repeat(async () => {
                // Note: No 'using' or other means of disposing it.
                // This does not cause any session leaks.
                var db = CreateContext();
                var singer = await db.Singers.FindAsync(1L);
                Assert.Equal(1L, singer?.SingerId);
            });
        }

        [Fact]
        public async Task QueryWithoutGettingResults_DoesNotLeakSession()
        {
            using var db = CreateContext();
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`" +
                      $"{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}WHERE STRPOS(`s`.`FirstName`, @__firstName_0) > 0";
            AddFindSingerResult(sql);

            await Repeat(() =>
            {
                // Create a query without actually executing it / getting the results.
                var firstName = "Alice";
                // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
                db.Singers.Where(s => s.FirstName.Contains(firstName));
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task OnlyDisposingReadOnlyTransactionWithoutCommitting_LeaksSession()
        {
            AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            var exception = await Assert.ThrowsAsync<SpannerException>(() => Repeat(async () =>
            {
                using var db = CreateContext();
                // NOTE: This transaction is being disposed, but it's not being committed or rolled back.
                using var transaction = await db.Database.BeginReadOnlyTransactionAsync();
                Assert.NotNull(await db.Singers.FindAsync(1L));
                // Note: No Commit or Rollback
            }));
            Assert.Equal(ErrorCode.ResourceExhausted, exception.ErrorCode);
        }

        private void AddFindSingerResult(string sql)
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
        }

        private void AddSelectSingerFullNameResult(string fullName, int paramIndex)
        {
            var selectFullNameSql = $"{Environment.NewLine}SELECT `FullName`{Environment.NewLine}FROM `Singers`" +
                $"{Environment.NewLine}WHERE  TRUE  AND `SingerId` = @p{paramIndex}";
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
        }
    }
}
#pragma warning restore EF1001
