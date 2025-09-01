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
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Google.Cloud.Spanner.Admin.Database.V1;
using Xunit;
using SpannerDate = Google.Cloud.EntityFrameworkCore.Spanner.Storage.SpannerDate;
using V1 = Google.Cloud.Spanner.V1;

#pragma warning disable EF1001
namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
    internal class MockServerSampleDbContext : SpannerSampleDbContext
    {
        private readonly string _connectionString;

        internal MockServerSampleDbContext(string connectionString)
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                    .UseMutations(MutationUsage.Never)
                    .UseLazyLoadingProxies();
            }
        }
    }

    internal class MockServerVersionDbContext : SpannerVersionDbContext
    {
        private readonly string _connectionString;

        internal MockServerVersionDbContext(string connectionString)
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                    .UseMutations(MutationUsage.Never)
                    .UseLazyLoadingProxies();
            }
        }
    }

    /// <summary>
    /// Tests CRUD operations using an in-mem Spanner mock server.
    /// </summary>
    [SuppressMessage("ReSharper", "ParameterOnlyUsedForPreconditionCheck.Local")]
    public class EntityFrameworkMockServerTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public EntityFrameworkMockServerTests(SpannerMockServerFixture service)
        {
            _fixture = service;
            service.SpannerMock.Reset();
        }

        //private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";
        private string ConnectionString => $"{_fixture.Host}:{_fixture.Port}/projects/p1/instances/i1/databases/d1;usePlainText=true";

        bool UsesClientLib()
        {
            return ConnectionString.StartsWith("Data Source=", StringComparison.Ordinal);
        }

        [Fact]
        public async Task FindSingerAsync_ReturnsNull_IfNotFound()
        {
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.Type, string>>(),
                new List<object[]>()));

            using var db = new MockServerSampleDbContext(ConnectionString);
            var singer = await db.Singers.FindAsync(1L);
            Assert.Null(singer);
        }

        [Fact]
        public async Task FindSingersUsingListOfIds_UsesParameterizedQuery()
        {
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                      $"FROM `Singers` AS `s`{Environment.NewLine}" +
                      $"WHERE `s`.`SingerId` IN  UNNEST (@__singerIds_0)";
            AddFindSingerResult(sql);

            var singerIds = new List<long>{1L, 2L, 3L};
            using var db = new MockServerSampleDbContext(ConnectionString);
            var singers = await db.Singers.Where(singer => singerIds.Contains(singer.SingerId)).ToListAsync();
            Assert.Single(singers);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Single(request.Params.Fields);
                    var fields = request.Params.Fields;
                    Assert.Collection(fields["__singerIds_0"].ListValue.Values,
                        v => Assert.Equal("1", v.StringValue),
                        v => Assert.Equal("2", v.StringValue),
                        v => Assert.Equal("3", v.StringValue)
                    );
                    Assert.Empty(request.ParamTypes);
                    // Assert.Single(request.ParamTypes);
                    // var type = request.ParamTypes["singerIds_0"];
                    // Assert.Equal(V1.TypeCode.Array, type.Code);
                    // Assert.Equal(V1.TypeCode.Int64, type.ArrayElementType.Code);
                }
            );
        }

        [Fact]
        public async Task FindSingersUsingListOfIntegers_UsesParameterizedQuery()
        {
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                      $"FROM `Singers` AS `s`{Environment.NewLine}" +
                      $"WHERE CAST(`s`.`SingerId` AS INT64) IN  UNNEST (@__singerIds_0)";
            AddFindSingerResult(sql);

            var singerIds = new List<int>{8, 9, 10};
            using var db = new MockServerSampleDbContext(ConnectionString);
            var singers = await db.Singers.Where(singer => singerIds.Contains((int) singer.SingerId)).ToListAsync();
            Assert.Single(singers);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Single(request.Params.Fields);
                    var fields = request.Params.Fields;
                    Assert.Collection(fields["__singerIds_0"].ListValue.Values,
                        v => Assert.Equal("8", v.StringValue),
                        v => Assert.Equal("9", v.StringValue),
                        v => Assert.Equal("10", v.StringValue)
                    );
                    Assert.Empty(request.ParamTypes);
                    // Assert.Single(request.ParamTypes);
                    // var type = request.ParamTypes["singerIds_0"];
                    // Assert.Equal(V1.TypeCode.Array, type.Code);
                    // Assert.Equal(V1.TypeCode.Int64, type.ArrayElementType.Code);
                }
            );
        }

        [Fact]
        public async Task FindPerformancesByType_UsesParameterizedQuery()
        {
            var sql = $"SELECT `p`.`VenueCode`, `p`.`SingerId`, `p`.`StartTime`, `p`.`AlbumId`, `p`.`ConcertStartTime`, `p`.`PerformanceType`, `p`.`Rating`, `p`.`TrackId`{Environment.NewLine}" +
                      $"FROM `Performances` AS `p`{Environment.NewLine}" +
                      $"WHERE `p`.`PerformanceType` = @__type_0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "VenueCode"),
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.Timestamp, "StartTime"),
                    Tuple.Create(V1.TypeCode.Int64, "AlbumId"),
                    Tuple.Create(V1.TypeCode.Timestamp, "ConcertStartTime"),
                    Tuple.Create(V1.TypeCode.Int64, "PerformanceType"),
                    Tuple.Create(V1.TypeCode.Float64, "Rating"),
                    Tuple.Create(V1.TypeCode.Int64, "TrackId"),
                },
                new List<object[]>
                {
                    new object[] { "VC", "1", "2025-02-18T14:00:00Z", "1", "2025-02-18T14:00:00Z", "0", 8.9, "1" },
                }
            ));

            var type = PerformanceType.Live;
            await using var db = new MockServerSampleDbContext(ConnectionString);
            var performances = await db.Performances.Where(performances => performances.PerformanceType == type).ToListAsync();
            Assert.Single(performances);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Single(request.Params.Fields);
                    var fields = request.Params.Fields;
                    Assert.Equal("0", fields["__type_0"].StringValue);
                    Assert.Empty(request.ParamTypes);
                    // Assert.Single(request.ParamTypes);
                    // var requestType = request.ParamTypes["__type_0"];
                    // Assert.Equal(V1.TypeCode.Int64, requestType.Code);
                }
            );
        }

        [Fact]
        public async Task FindPerformancesByCollectionOfTypes_UsesParameterizedQuery()
        {
            var sql = $"SELECT `p`.`VenueCode`, `p`.`SingerId`, `p`.`StartTime`, `p`.`AlbumId`, `p`.`ConcertStartTime`, `p`.`PerformanceType`, `p`.`Rating`, `p`.`TrackId`{Environment.NewLine}" +
                      $"FROM `Performances` AS `p`{Environment.NewLine}" +
                      $"WHERE CAST(`p`.`PerformanceType` AS INT64) IN  UNNEST (@__typesAsInts_0)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "VenueCode"),
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.Timestamp, "StartTime"),
                    Tuple.Create(V1.TypeCode.Int64, "AlbumId"),
                    Tuple.Create(V1.TypeCode.Timestamp, "ConcertStartTime"),
                    Tuple.Create(V1.TypeCode.Int64, "PerformanceType"),
                    Tuple.Create(V1.TypeCode.Float64, "Rating"),
                    Tuple.Create(V1.TypeCode.Int64, "TrackId"),
                },
                new List<object[]>
                {
                    new object[] { "VC", "1", "2025-02-18T14:00:00Z", "1", "2025-02-18T14:00:00Z", "0", 8.9, "1" },
                }
            ));

            var types = new List<PerformanceType>{PerformanceType.Live, PerformanceType.Playback};
            var typesAsInts = types.ConvertAll(t => (int)t);
            await using var db = new MockServerSampleDbContext(ConnectionString);
            var performances = await db.Performances.Where(performances => typesAsInts.Contains((int) performances.PerformanceType)).ToListAsync();
            Assert.Single(performances);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Single(request.Params.Fields);
                    var fields = request.Params.Fields;
                    Assert.Equal(2, fields["__typesAsInts_0"].ListValue.Values.Count);
                    Assert.Equal("0", fields["__typesAsInts_0"].ListValue.Values[0].StringValue);
                    Assert.Equal("1", fields["__typesAsInts_0"].ListValue.Values[1].StringValue);
                    Assert.Empty(request.ParamTypes);
                    // Assert.Single(request.ParamTypes);
                    // var type = request.ParamTypes["__typesAsInts_0"];
                    // Assert.Equal(V1.TypeCode.Array, type.Code);
                    // Assert.Equal(V1.TypeCode.Int64, type.ArrayElementType.Code);
                }
            );
        }

        [Fact]
        public async Task FindSingerAsync_ReturnsInstance_IfFound()
        {
            var sql = AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            await using var db = new MockServerSampleDbContext(ConnectionString);
            var singer = await db.Singers.FindAsync(1L);
            Assert.NotNull(singer);
            Assert.Equal(1L, singer.SingerId);
            Assert.Null(singer.BirthDate);
            Assert.Equal("Alice", singer.FirstName);
            Assert.Equal("Alice Morrison", singer.FullName);
            Assert.Equal("Morrison", singer.LastName);
            Assert.Null(singer.Picture);

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal(new TransactionOptions{ReadOnly = new TransactionOptions.Types.ReadOnly{Strong = true, ReturnReadTimestamp = true}}, request.Transaction.SingleUse);
                    //Assert.Null(request.Transaction);
                }
            );
            // A read-only operation should not initiate and commit a transaction.
            Assert.DoesNotContain(_fixture.SpannerMock.Requests, request => request is CommitRequest);
        }

        [Fact]
        public async Task InsertSinger_SelectsFullName()
        {
            // Setup results.
            var insertSql = $"INSERT INTO `Singers` (`SingerId`, `BirthDate`, `FirstName`, `LastName`, `Picture`)" +
                $"{Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4){Environment.NewLine}" +
                $"THEN RETURN `FullName`{Environment.NewLine}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type {Code = V1.TypeCode.String}, "FullName", "Alice Morrison"));

            await using var db = new MockServerSampleDbContext(ConnectionString);
            db.Singers.Add(new Singers
            {
                SingerId = 1L,
                FirstName = "Alice",
                LastName = "Morrison",
            });
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            var useInlineBegin = true;
            if (useInlineBegin)
            {
                Assert.Empty(_fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>());
                Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
                Assert.Collection(
                    _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                    request =>
                    {
                        Assert.Equal(insertSql, request.Sql);
                        Assert.False(request.Transaction.HasId);
                        Assert.Equal(TransactionSelector.SelectorOneofCase.Begin, request.Transaction.SelectorCase);
                        Assert.Equal(TransactionOptions.ModeOneofCase.ReadWrite, request.Transaction.Begin.ModeCase);
                    }
                );
            }
            else
            {
                Assert.Single(_fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>());
                Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
                Assert.Collection(
                    _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                    request =>
                    {
                        Assert.Equal(insertSql, request.Sql);
                        Assert.True(request.Transaction.HasId);
                    }
                );
            }

            Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);

            Assert.Collection(_fixture.SpannerMock.Requests
                .Where(request => request is ExecuteBatchDmlRequest || request is CommitRequest || request is ExecuteSqlRequest)
                .Select(request => request.GetType()),
                request => Assert.Equal(typeof(ExecuteSqlRequest), request),
                request => Assert.Equal(typeof(CommitRequest), request)
            );

        }

        [Fact]
        public async Task InsertTicketSale_ReturnsId()
        {
            // Setup results.
            var insertSql = $"INSERT INTO `TicketSales` (`CustomerName`){Environment.NewLine}" +
                            $"VALUES (@p0){Environment.NewLine}" +
                            $"THEN RETURN `Id`{Environment.NewLine}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type {Code = V1.TypeCode.Int64}, "Id", "12345"));

            await using var db = new MockServerSampleDbContext(ConnectionString);
            var ticketSale = db.TicketSales.Add(new TicketSales
            {
                CustomerName = "New Customer",
            });
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            Assert.Equal(12345L, ticketSale.Entity.Id);
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(insertSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);

            Assert.Collection(_fixture.SpannerMock.Requests
                    .Where(request => request is ExecuteBatchDmlRequest || request is CommitRequest || request is ExecuteSqlRequest)
                    .Select(request => request.GetType()),
                request => Assert.Equal(typeof(ExecuteSqlRequest), request),
                request => Assert.Equal(typeof(CommitRequest), request)
            );

        }

        [Fact]
        public async Task InsertMultipleTicketSale_ReturnsIdentifiers()
        {
            // Setup results.
            for (var p = 0; p < 3; p++)
            {
                var insertSql = $"INSERT INTO `TicketSales` (`CustomerName`){Environment.NewLine}" +
                                $"VALUES (@p{p}){Environment.NewLine}" +
                                $"THEN RETURN `Id`{Environment.NewLine}";
                _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql,
                    StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id",
                        (1000000 - p)));
            }

            await using var db = new MockServerSampleDbContext(ConnectionString);
            var transaction = await db.Database.BeginTransactionAsync();
            db.TicketSales.AddRange(
                new TicketSales { CustomerName = "New Customer1"},
                new TicketSales { CustomerName = "New Customer2"},
                new TicketSales { CustomerName = "New Customer3"}
            );
            var updateCount = await db.SaveChangesAsync();
            await transaction.CommitAsync();

            Assert.Equal(3L, updateCount);
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request => { Assert.NotNull(request.Transaction?.Id); },
                request => { Assert.NotNull(request.Transaction?.Id); },
                request => { Assert.NotNull(request.Transaction?.Id); }
            );
            Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);

            Assert.Collection(_fixture.SpannerMock.Requests
                    .Where(request => request is ExecuteBatchDmlRequest || request is CommitRequest || request is ExecuteSqlRequest)
                    .Select(request => request.GetType()),
                request => Assert.Equal(typeof(ExecuteSqlRequest), request),
                request => Assert.Equal(typeof(ExecuteSqlRequest), request),
                request => Assert.Equal(typeof(ExecuteSqlRequest), request),
                request => Assert.Equal(typeof(CommitRequest), request)
            );
        }

        [Fact]
        public async Task UpdateSinger_SelectsFullName()
        {
            // Setup results.
            var updateSql = $"UPDATE `Singers` SET `LastName` = @p0{Environment.NewLine}" +
                            $"WHERE `SingerId` = @p1{Environment.NewLine}" +
                            $"THEN RETURN `FullName`{Environment.NewLine}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type{Code = V1.TypeCode.String}, "FullName", "Alice Pieterson-Morrison"));
            var selectSingerSql = AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, " +
                $"`s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");

            using var db = new MockServerSampleDbContext(ConnectionString);
            var singer = await db.Singers.FindAsync(1L);
            Assert.NotNull(singer);
            singer.LastName = "Pieterson-Morrison";
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            var useInlineBegin = false;
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(selectSingerSql, request.Sql);
                    if (useInlineBegin)
                    {
                        Assert.Null(request.Transaction?.Id);
                    }
                    else
                    {
                        Assert.NotNull(request.Transaction?.Id);
                    }
                },
                request =>
                {
                    Assert.Equal(updateSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);
        }

        [Fact]
        public async Task DeleteSinger_DoesNotSelectFullName()
        {
            // Setup results.
            var deleteSql = $"DELETE FROM `Singers`{Environment.NewLine}WHERE `SingerId` = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(deleteSql, StatementResult.CreateUpdateCount(1L));

            using var db = new MockServerSampleDbContext(ConnectionString);
            db.Singers.Remove(new Singers { SingerId = 1L });
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            var beginRequests = _fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>();
            Assert.Empty(beginRequests);
            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>();
            Assert.Collection(
                requests,
                request =>
                {
                    Assert.Single(request.Statements);
                    Assert.Equal(deleteSql, request.Statements[0].Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);
        }

        [Fact]
        public async Task InsertTrack_SelectsRecordedAt()
        {
            // Setup results.
            var insertSql = $"INSERT INTO `Tracks` (`AlbumId`, `TrackId`, `Duration`, `Lyrics`, `LyricsLanguages`, `Title`){Environment.NewLine}" +
                            $"VALUES (@p0, @p1, @p2, @p3, @p4, @p5){Environment.NewLine}" +
                            $"THEN RETURN `RecordedAt`{Environment.NewLine}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type {Code = V1.TypeCode.Timestamp}, "RecordedAt", DateTime.Now));

            using var db = new MockServerSampleDbContext(ConnectionString);
            db.Tracks.Add(new Tracks
            {
                AlbumId = 1L,
                TrackId = 1L,
                Title = "Test title",
            });
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(insertSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);

            Assert.Collection(_fixture.SpannerMock.Requests
                .Where(request => request is ExecuteBatchDmlRequest || request is CommitRequest || request is ExecuteSqlRequest)
                .Select(request => request.GetType()),
                request => Assert.Equal(typeof(ExecuteSqlRequest), request),
                request => Assert.Equal(typeof(CommitRequest), request)
            );

        }

        [Fact]
        public async Task CanUseReadOnlyTransaction()
        {
            var sql = AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");
            await using var db = new MockServerSampleDbContext(ConnectionString);
            await db.Database.OpenConnectionAsync();
            await using var transaction = await db.Database.BeginReadOnlyTransactionAsync();

            Assert.NotNull(await db.Singers.FindAsync(1L));

            var useInlineBegin = true;
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.NotNull(request.Transaction);
                    if (useInlineBegin)
                    {
                        Assert.False(request.Transaction.HasId);
                        Assert.Equal(TransactionSelector.SelectorOneofCase.Begin, request.Transaction.SelectorCase);
                        Assert.Equal(TransactionOptions.ModeOneofCase.ReadOnly, request.Transaction.Begin.ModeCase);
                        Assert.True(request.Transaction.Begin.ReadOnly.HasStrong);
                    }
                    else
                    {
                        Assert.True(request.Transaction.HasId);
                    }
                }
            );
            if (useInlineBegin)
            {
                Assert.Empty(_fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>());
            }
            else
            {
                Assert.Collection(_fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>(),
                request => {
                    Assert.Equal(TransactionOptions.ModeOneofCase.ReadOnly, request.Options.ModeCase);
                    Assert.True(request.Options.ReadOnly.HasStrong);
                });
            }
        }

        [Fact]
        public async Task CanUseReadOnlyTransactionWithTimestampBound()
        {
            var sql = AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`," +
                $" `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");
            await using var db = new MockServerSampleDbContext(ConnectionString);
            await db.Database.OpenConnectionAsync();
            await using var transaction = await db.Database.BeginReadOnlyTransactionAsync(TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(10)));

            Assert.NotNull(await db.Singers.FindAsync(1L));

            var useInlineBegin = true;
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.NotNull(request.Transaction);
                    if (useInlineBegin)
                    {
                        Assert.False(request.Transaction.HasId);
                        Assert.Equal(TransactionSelector.SelectorOneofCase.Begin, request.Transaction.SelectorCase);
                        Assert.Equal(TransactionOptions.ModeOneofCase.ReadOnly, request.Transaction.Begin.ModeCase);
                        Assert.Equal(TransactionOptions.Types.ReadOnly.TimestampBoundOneofCase.ExactStaleness,
                            request.Transaction.Begin.ReadOnly.TimestampBoundCase);
                        Assert.Equal(10, request.Transaction.Begin.ReadOnly.ExactStaleness.Seconds);
                    }
                    else
                    {
                        Assert.True(request.Transaction.HasId);
                    }
                }
            );
            if (useInlineBegin)
            {
                Assert.Empty(_fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>());
            }
            else
            {
                Assert.Collection(_fixture.SpannerMock.Requests.OfType<BeginTransactionRequest>(),
                    request =>
                    {
                        Assert.Equal(TransactionOptions.ModeOneofCase.ReadOnly, request.Options.ModeCase);
                        Assert.Equal(TransactionOptions.Types.ReadOnly.TimestampBoundOneofCase.ExactStaleness,
                            request.Options.ReadOnly.TimestampBoundCase);
                        Assert.Equal(10, request.Options.ReadOnly.ExactStaleness.Seconds);
                    });
            }
        }

        [Fact]
        public async Task CanReadWithMaxStaleness()
        {
            var sql = AddFindSingerResult($"-- max_staleness: 10{Environment.NewLine}{Environment.NewLine}" +
                                          $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                                          $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                          $"WHERE `s`.`SingerId` = @__id_0{Environment.NewLine}LIMIT 1");

            using var db = new MockServerSampleDbContext(ConnectionString);
            var id = 1L;
            await db.Singers.WithTimestampBound(TimestampBound.OfMaxStaleness(TimeSpan.FromSeconds(10))).Where(s => s.SingerId == id).FirstAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal(Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
                        request.Transaction?.SingleUse?.ReadOnly?.MaxStaleness);
                }
            );
        }

        [Fact]
        public async Task CanReadWithExactStaleness()
        {
            var sql = AddFindSingerResult($"-- exact_staleness: 5{CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator}5{Environment.NewLine}{Environment.NewLine}" +
                                          $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                                          $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                          $"WHERE `s`.`SingerId` = @__id_0{Environment.NewLine}LIMIT 1");

            await using var db = new MockServerSampleDbContext(ConnectionString);
            var id = 1L;
            await db.Singers.WithTimestampBound(TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(5.5))).Where(s => s.SingerId == id).FirstAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal(Duration.FromTimeSpan(TimeSpan.FromSeconds(5.5)), request.Transaction?.SingleUse?.ReadOnly?.ExactStaleness);
                }
            );
        }

        [Fact]
        public async Task CanReadWithMinReadTimestamp()
        {
            var sql = AddFindSingerResult($"-- min_read_timestamp: 2021-09-08T15:18:01.1230000Z{Environment.NewLine}{Environment.NewLine}" +
                                          $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                                          $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                          $"WHERE `s`.`SingerId` = @__id_0{Environment.NewLine}LIMIT 1");

            using var db = new MockServerSampleDbContext(ConnectionString);
            var id = 1L;
            await db.Singers
                .WithTimestampBound(TimestampBound.OfMinReadTimestamp(DateTime.Parse("2021-09-08T17:18:01.123+02:00").ToUniversalTime()))
                .Where(s => s.SingerId == id).FirstAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal(Timestamp.FromDateTime(new DateTime(2021, 9, 8, 15, 18, 1, 123, DateTimeKind.Utc)), request.Transaction?.SingleUse?.ReadOnly?.MinReadTimestamp);
                }
            );
        }

        [Fact]
        public async Task CanReadWithReadTimestamp()
        {
            var sql = AddFindSingerResult($"-- read_timestamp: 2021-09-08T15:18:02.0000000Z{Environment.NewLine}{Environment.NewLine}" +
                                          $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, " +
                                          $"`s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                                          $"WHERE `s`.`SingerId` = @__id_0{Environment.NewLine}LIMIT 1");

            using var db = new MockServerSampleDbContext(ConnectionString);
            var id = 1L;
            await db.Singers
                .WithTimestampBound(TimestampBound.OfReadTimestamp(DateTime.Parse("2021-09-08T15:18:02Z").ToUniversalTime()))
                .Where(s => s.SingerId == id).FirstAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal(Timestamp.FromDateTime(new DateTime(2021, 9, 8, 15, 18, 2, DateTimeKind.Utc)), request.Transaction?.SingleUse?.ReadOnly?.ReadTimestamp);
                }
            );
        }

        [Fact]
        public async Task InsertUsingRawSqlReturnsUpdateCountWithoutAdditionalSelectCommand()
        {
            await using var db = new MockServerSampleDbContext(ConnectionString);
            var today = SpannerDate.FromDateTime(DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Unspecified));
            var now = DateTime.UtcNow;
            var id = 1L;
            var rawSql = @"INSERT INTO `TableWithAllColumnTypes` 
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
                ColJson = JsonDocument.Parse("{\"key1\": \"value1\", \"key2\": \"value2\"}"),
                ColJsonArray = new List<JsonDocument>{ JsonDocument.Parse("{\"key1\": \"value1\", \"key2\": \"value2\"}"), JsonDocument.Parse("{\"key1\": \"value3\", \"key2\": \"value4\"}") },
            };
            int updateCount;
            if (UsesClientLib())
            {
                updateCount = await db.Database.ExecuteSqlRawAsync(rawSql,
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
            }
            else
            {
                updateCount = await db.Database.ExecuteSqlRawAsync(rawSql,
                    row.ColBool,
                    row.ColBoolArray,
                    row.ColBytes,
                    row.ColBytesMax,
                    row.ColBytesArray,
                    row.ColBytesMaxArray,
                    row.ColDate,
                    row.ColDateArray,
                    row.ColFloat64,
                    row.ColFloat64Array,
                    row.ColInt64,
                    row.ColInt64Array,
                    row.ColNumeric.Value.ToDecimal(LossOfPrecisionHandling.Truncate),
                    row.ColNumericArray.Select(d => d.Value.ToDecimal(LossOfPrecisionHandling.Truncate)).ToList(),
                    row.ColString,
                    row.ColStringArray,
                    row.ColStringMax,
                    row.ColStringMaxArray,
                    row.ColTimestamp,
                    row.ColTimestampArray,
                    row.ColJson?.ToString(),
                    row.ColJsonArray?.Select(d => d?.ToString()).ToList()
                );
            }

            Assert.Equal(1, updateCount);
            // Verify that the INSERT statement is the only one on the mock server.
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request => Assert.Equal(rawSql, request.Sql)
            );
        }

        [Fact]
        public async Task VersionNumberIsAutomaticallyGeneratedOnInsertAndUpdate()
        {
            var insertSql = $"INSERT INTO `SingersWithVersion` (`SingerId`, `FirstName`, `LastName`, `Version`)" +
                $"{Environment.NewLine}VALUES (@p0, @p1, @p2, @p3)";
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
            var updateSql = $"UPDATE `SingersWithVersion` SET `LastName` = @p0, `Version` = @p1" +
                $"{Environment.NewLine}WHERE `SingerId` = @p2 AND `Version` = @p3";
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
            var updateSql = $"UPDATE `SingersWithVersion` SET `LastName` = @p0, `Version` = @p1" +
                $"{Environment.NewLine}WHERE `SingerId` = @p2 AND `Version` = @p3";
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
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "c", 1));
            var insertSql = $"INSERT INTO `Venues` (`Code`, `Active`, `Capacity`, `Name`, `Ratings`)" +
                            $"{Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

            await using var db = new MockServerSampleDbContext(ConnectionString);
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

            // We can only disable internal retries when using explicit transactions. Otherwise, internal retries
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
                if (UsesClientLib())
                {
                    var e = await Assert.ThrowsAsync<SpannerException>(() => db.SaveChangesAsync());
                    Assert.Equal(ErrorCode.Aborted, e.ErrorCode);
                }
                else
                {
                    var e = await Assert.ThrowsAsync<SpannerLib.SpannerException>(() => db.SaveChangesAsync());
                    Assert.Equal(SpannerLib.ErrorCode.Aborted, e.ErrorCode);
                }
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
                Assert.Collection(
                    _fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>(),
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
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "c", 1));
            var insertSql = $"INSERT INTO `Venues` (`Code`, `Active`, `Capacity`, `Name`, `Ratings`)" +
                            $"{Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

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
                // The transaction must have been initialized for it to fail at all. Otherwise, the client library
                // will automatically retry the statement.
                var cmd = transaction.GetDbTransaction().Connection!.CreateCommand();
                cmd.CommandText = "SELECT 1";
                cmd.Transaction = transaction.GetDbTransaction();
                await cmd.ExecuteScalarAsync();
                // Abort the next statement that is executed on the mock server.
                _fixture.SpannerMock.AbortNextStatement();
                if (UsesClientLib())
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
                    var e = await Assert.ThrowsAsync<SpannerLib.SpannerException>(
                        () => db.Database.ExecuteSqlRawAsync(insertSql, "C1", true, 1000L, "Concert Hall", null)
                    );
                    Assert.Equal(SpannerLib.ErrorCode.Aborted, e.ErrorCode);
                }
            }
            else
            {
                // Abort the next statement that is executed on the mock server.
                _fixture.SpannerMock.AbortNextStatement();
                int updateCount;
                if (UsesClientLib())
                {
                    updateCount = await db.Database.ExecuteSqlRawAsync(insertSql,
                        new SpannerParameter("p0", SpannerDbType.String, "C1"),
                        new SpannerParameter("p1", SpannerDbType.Bool, true),
                        new SpannerParameter("p2", SpannerDbType.Int64, 1000L),
                        new SpannerParameter("p3", SpannerDbType.String, "Concert Hall"),
                        new SpannerParameter("p4", SpannerDbType.ArrayOf(SpannerDbType.Float64))
                    );
                }
                else
                {
                    updateCount = await db.Database.ExecuteSqlRawAsync(insertSql, "C1", true, 1000L, "Concert Hall", null);
                }
                Assert.Equal(1L, updateCount);
                if (useExplicitTransaction)
                {
                    await transaction.CommitAsync();
                }
                Assert.Collection(
                    _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
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
            }
        }

        [Fact]
        public async Task CanUseLimitWithoutOffset()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}ORDER BY `s`.`LastName`" +
                $"{Environment.NewLine}LIMIT @__p_0";
            AddFindSingerResult(sql);

            var singers = await db.Singers
                .OrderBy(s => s.LastName)
                .Take(1)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
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
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"ORDER BY `s`.`LastName`{Environment.NewLine}LIMIT @__p_1 OFFSET @__p_0";
            AddFindSingerResult(sql);

            var singers = await db.Singers
                .OrderBy(s => s.LastName)
                .Skip(2)
                .Take(1)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
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
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"ORDER BY `s`.`LastName`{Environment.NewLine}LIMIT {long.MaxValue / 2} OFFSET @__p_0";
            AddFindSingerResult(sql);

            var singers = await db.Singers
                .OrderBy(s => s.LastName)
                .Skip(3)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
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
                    new object[] { 1L, null, "Zeke", "Zeke Peterson", "Peterson", null, 100L, new List<string>{"foo", "bar"}, null, 1L, "Some Title" },
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
                    new object[] { 3L, null, "Zeke", "Zeke Peterson", "Peterson", null, 100L, new List<string>{"foo", "bar"}, null, 3L, "Some Title" },
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
                    Assert.Equal(["foo", "bar"], s.Album.Awards);
                }
            );
        }

        [Fact]
        public async Task CanUseStringContains()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE STRPOS(`s`.`FirstName`, @__firstName_0) > 0";
            AddFindSingerResult(sql);

            var firstName = "Alice";
            var singers = await db.Singers
                .Where(s => s.FirstName.Contains(firstName))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
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
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE STARTS_WITH(`s`.`FullName`, @__fullName_0)";
            AddFindSingerResult(sql);

            var fullName = "Alice M";
            var singers = await db.Singers
                .Where(s => s.FullName.StartsWith(fullName))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
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
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE ENDS_WITH(`s`.`FullName`, @__fullName_0)";
            AddFindSingerResult(sql);

            var fullName = " Morrison";
            var singers = await db.Singers
                .Where(s => s.FullName.EndsWith(fullName))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
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
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE CHAR_LENGTH(`s`.`FirstName`) > @__minLength_0";
            AddFindSingerResult(sql);

            var minLength = 4;
            var singers = await db.Singers
                .Where(s => s.FirstName.Length > minLength)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Morrison", s.LastName));
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal("4", request.Params.Fields["__minLength_0"].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanUseStringConcat()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT CONCAT(`s`.`FirstName`, ''' ''', `s`.`LastName`, CAST(`s`.`SingerId` AS STRING)){Environment.NewLine}" + 
                      $"FROM `Singers` AS `s`{Environment.NewLine}" + 
                      "LIMIT 1";
            AddFindSingerResult(sql);

            await db.Singers
                .Select(s => string.Concat(s.FirstName, " ", s.LastName, s.SingerId.ToString()))
                .FirstOrDefaultAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                }
            );
        }

        [Fact]
        public async Task CanUseStringPlus()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT (COALESCE(`s`.`FirstName`, '''''')||''' ''')||`s`.`LastName`{Environment.NewLine}" + 
                      $"FROM `Singers` AS `s`{Environment.NewLine}" + 
                      "LIMIT 1";
            AddFindSingerResult(sql);

            await db.Singers
                .Select(s => s.FirstName + " " + s.LastName)
                .FirstOrDefaultAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                }
            );
        }

        [Fact]
        public async Task CanUseRegexReplace()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
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
            var startTimes = await db.Singers
                .Where(s => s.SingerId == singerId && s.BirthDate != null)
                .Select(s => s.BirthDate.Value.AddYears(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new SpannerDate(1980, 1, 20), s));
        }

        [Fact]
        public async Task CanUseDateTimeAddMonths()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
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
            var startTimes = await db.Singers
                .Where(s => s.SingerId == singerId && s.BirthDate != null)
                .Select(s => s.BirthDate.Value.AddMonths(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new SpannerDate(1980, 1, 20), s));
        }

        [Fact]
        public async Task CanUseDateTimeAddDays()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
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
            var startTimes = await db.Singers
                .Where(s => s.SingerId == singerId && s.BirthDate != null)
                .Select(s => s.BirthDate.Value.AddDays(1))
                .ToListAsync();
            Assert.Collection(startTimes, s => Assert.Equal(new SpannerDate(1980, 1, 20), s));
        }

        [Fact]
        public async Task CanUseDateTimeAddHours()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
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
            var sql = "SELECT EXTRACT(YEAR FROM COALESCE(`t`.`ColDate`, DATE '0001-01-01')) AS `Year`, " +
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
            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                // ReSharper disable once SpecifyACultureInStringConversionExplicitly
                .Select(t => t.ColFloat64.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("3.0", converted);
        }

        [Fact]
        public async Task CanUseSpannerDateToString()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
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
            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                // ReSharper disable once SpecifyACultureInStringConversionExplicitly
                .Select(t => t.ColDate.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("2021-01-25", converted);
        }

        [Fact]
        public async Task CanUseDateTimeToString()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
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
            var converted = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64 == id)
                // ReSharper disable once SpecifyACultureInStringConversionExplicitly
                .Select(t => t.ColTimestamp.GetValueOrDefault().ToString())
                .FirstOrDefaultAsync();
            Assert.Equal("2021-01-25T12:46:01.982784Z", converted);
        }

        [Fact]
        public async Task CanUseLongListCount()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColInt64Array`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            var id = await db.TableWithAllColumnTypes
                .Where(t => t.ColInt64Array.Count == 2)
                .Select(t => t.ColInt64)
                .FirstOrDefaultAsync();
            Assert.Equal(1L, id);
        }

        [Fact]
        public async Task CanUseDoubleListCount()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColFloat64Array`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            var id = await db.TableWithAllColumnTypes
                .Where(t => t.ColFloat64Array.Count == 2)
                .Select(t => t.ColInt64)
                .FirstOrDefaultAsync();
            Assert.Equal(1L, id);
        }

        [Fact]
        public async Task CanUseSpannerNumericListCount()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColNumericArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            var id = await db.TableWithAllColumnTypes
                .Where(t => t.ColNumericArray.Count == 2)
                .Select(t => t.ColInt64)
                .FirstOrDefaultAsync();
            Assert.Equal(1L, id);
        }

        [Fact]
        public async Task CanUseBoolListCount()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColBoolArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            var id = await db.TableWithAllColumnTypes
                .Where(t => t.ColBoolArray.Count == 2)
                .Select(t => t.ColInt64)
                .FirstOrDefaultAsync();
            Assert.Equal(1L, id);
        }

        [Fact]
        public async Task CanUseStringListCount()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColStringArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            var id = await db.TableWithAllColumnTypes
                .Where(t => t.ColStringArray.Count == 2)
                .Select(t => t.ColInt64)
                .FirstOrDefaultAsync();
            Assert.Equal(1L, id);
        }

        [Fact]
        public async Task CanUseSByteArrayListCount()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColBytesArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            var id = await db.TableWithAllColumnTypes
                .Where(t => t.ColBytesArray.Count == 2)
                .Select(t => t.ColInt64)
                .FirstOrDefaultAsync();
            Assert.Equal(1L, id);
        }

        [Fact]
        public async Task CanUseSpannerDateListCount()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColDateArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            var id = await db.TableWithAllColumnTypes
                .Where(t => t.ColDateArray.Count == 2)
                .Select(t => t.ColInt64)
                .FirstOrDefaultAsync();
            Assert.Equal(1L, id);
        }

        [Fact]
        public async Task CanUseDateTimeListCount()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `t`.`ColInt64`{Environment.NewLine}FROM `TableWithAllColumnTypes` AS `t`" +
                $"{Environment.NewLine}WHERE ARRAY_LENGTH(`t`.`ColTimestampArray`) = 2{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSelect1ResultSet());

            var id = await db.TableWithAllColumnTypes
                .Where(t => t.ColTimestampArray.Count == 2)
                .Select(t => t.ColInt64)
                .FirstOrDefaultAsync();
            Assert.Equal(1L, id);
        }

        [Fact]
        public async Task CanInsertCommitTimestamp()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = "INSERT INTO `TableWithAllColumnTypes` (`ColCommitTS`, `ColInt64`, `ASC`, `ColBool`, " +
                "`ColBoolArray`, `ColBytes`, `ColBytesArray`, `ColBytesMax`, `ColBytesMaxArray`, `ColDate`, `ColDateArray`," +
                " `ColFloat32`, `ColFloat32Array`, `ColFloat64`, `ColFloat64Array`, `ColInt64Array`, `ColJson`, `ColJsonArray`, `ColNumeric`, `ColNumericArray`, `ColString`, " +
                "`ColStringArray`, `ColStringMax`, `ColStringMaxArray`, `ColTimestamp`, `ColTimestampArray`)" +
                $"{Environment.NewLine}VALUES (PENDING_COMMIT_TIMESTAMP(), " +
                $"@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23, @p24){Environment.NewLine}" +
                $"THEN RETURN `ColComputed`";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.String }, "ColComputed", "Test"));

            db.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes { ColInt64 = 1L });
            await db.SaveChangesAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Contains("PENDING_COMMIT_TIMESTAMP()", request.Sql);
                }
            );
        }

        [Fact]
        public async Task CanUpdateCommitTimestamp()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"UPDATE `TableWithAllColumnTypes` SET `ColCommitTS` = PENDING_COMMIT_TIMESTAMP(), `ColBool` = @p0" +
                $"{Environment.NewLine}WHERE `ColInt64` = @p1{Environment.NewLine}" +
                $"THEN RETURN `ColComputed`";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.String }, "ColComputed", "Test"));

            var row = new TableWithAllColumnTypes { ColInt64 = 1L };
            db.TableWithAllColumnTypes.Attach(row);
            row.ColBool = true;
            await db.SaveChangesAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Contains("PENDING_COMMIT_TIMESTAMP()", request.Sql);
                }
            );
        }

        [Fact]
        public async Task CanUseAsAsyncEnumerable()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`" +
                $"{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}WHERE STRPOS(`s`.`FirstName`, @__firstName_0) > 0";
            AddFindSingerResult(sql);

            var firstName = "Alice";
            var singers = db.Singers
                .Where(s => s.FirstName.Contains(firstName))
                .AsAsyncEnumerable();
            await foreach (var singer in singers)
            {
                Assert.Equal("Morrison", singer.LastName);
            }
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Equal("Alice", request.Params.Fields["__firstName_0"].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanInsertRowWithCommitTimestampAndComputedColumn()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = "INSERT INTO `TableWithAllColumnTypes` (`ColCommitTS`, `ColInt64`, `ASC`, `ColBool`, `ColBoolArray`," +
                " `ColBytes`, `ColBytesArray`, `ColBytesMax`, `ColBytesMaxArray`, `ColDate`, `ColDateArray`, `ColFloat32`, `ColFloat32Array`," +
                " `ColFloat64`, `ColFloat64Array`, `ColInt64Array`, `ColJson`, `ColJsonArray`, `ColNumeric`, `ColNumericArray`, `ColString`, `ColStringArray`," +
                $" `ColStringMax`, `ColStringMaxArray`, `ColTimestamp`, `ColTimestampArray`){Environment.NewLine}" +
                $"VALUES (PENDING_COMMIT_TIMESTAMP(), @p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23, @p24){Environment.NewLine}" +
                $"THEN RETURN `ColComputed`{Environment.NewLine}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.String }, "ColComputed", "Test"));

            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = 1L }
            );
            await db.SaveChangesAsync();

            Assert.Collection(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(), request => Assert.Equal(sql, request.Sql));
            Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);
            // Verify the order of the requests (that is, the Select statement should be outside the implicit transaction).
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteBatchDmlRequest || request is CommitRequest || request is ExecuteSqlRequest).Select(request => request.GetType()),
                requestType => Assert.Equal(typeof(ExecuteSqlRequest), requestType),
                requestType => Assert.Equal(typeof(CommitRequest), requestType)
            );
        }

        [Fact]
        public async Task CanInsertAllTypes()
        {
            using var db = new MockServerSampleDbContext(ConnectionString);
            var sql = "INSERT INTO `TableWithAllColumnTypes` (`ColCommitTS`, `ColInt64`, `ASC`, `ColBool`, " +
                      "`ColBoolArray`, `ColBytes`, `ColBytesArray`, `ColBytesMax`, `ColBytesMaxArray`, `ColDate`, `ColDateArray`," +
                      " `ColFloat32`, `ColFloat32Array`, `ColFloat64`, `ColFloat64Array`, `ColInt64Array`, `ColJson`, `ColJsonArray`, `ColNumeric`, `ColNumericArray`, `ColString`, " +
                      "`ColStringArray`, `ColStringMax`, `ColStringMaxArray`, `ColTimestamp`, `ColTimestampArray`)" +
                      $"{Environment.NewLine}VALUES (PENDING_COMMIT_TIMESTAMP(), " +
                      $"@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20, @p21, @p22, @p23, @p24){Environment.NewLine}" +
                      $"THEN RETURN `ColComputed`{Environment.NewLine}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.String }, "ColComputed", "Test"));

            db.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes
            {
                ColInt64 = 1L,
                ColBool = true,
                ColBytes = new byte[] {1,2,3},
                ColDate = new SpannerDate(2000, 1, 1),
                ColFloat32 = 3.14f,
                ColFloat64 = 3.14,
                ColJson = JsonDocument.Parse("{\"key\": \"value\"}"),
                ColNumeric = SpannerNumeric.Parse("6.626"),
                ColString = "test",
                ColTimestamp = new DateTime(2000, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc),
                ColBoolArray = new List<bool?>{true, null, false},
                ColBytesArray = new List<byte[]>{new byte[]{1,2,3}, null, new byte[]{3,2,1}},
                ColBytesMax = new byte[] {},
                ColDateArray = new List<SpannerDate?>{new SpannerDate(2021, 8, 26), null, new SpannerDate(2000, 1, 1)},
                ColFloat32Array = new List<float?>{3.14f, null, 6.626f},
                ColFloat64Array = new List<double?>{3.14, null, 6.626},
                ColInt64Array = new List<long?>{1,null,2},
                ColJsonArray = new List<JsonDocument>{JsonDocument.Parse("{\"key1\": \"value1\"}"), null, JsonDocument.Parse("{\"key2\": \"value2\"}")},
                ColNumericArray = new List<SpannerNumeric?>{SpannerNumeric.Parse("3.14"), null, SpannerNumeric.Parse("6.626")},
                ColStringArray = new List<string>{"test1", null, "test2"},
                ColStringMax = "",
                ColTimestampArray = new List<DateTime?>{new DateTime(2000, 1, 1, 0, 0, 0, 1, DateTimeKind.Utc), null, new DateTime(2000, 1, 1, 0, 0, 0, 2, DateTimeKind.Utc)},
                ColBytesMaxArray = new List<byte[]>(),
                ColStringMaxArray = new List<string>(),
            });
            await db.SaveChangesAsync();

            // Verify the parameter types.
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    var types = request.ParamTypes;
                    if (UsesClientLib())
                    {
                        var index = -1;
                        Assert.Equal(24, types.Count);
                        Assert.Equal(V1.TypeCode.Int64, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.String, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Bool, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Bool, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Bytes, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Bytes, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Bytes, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Bytes, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Date, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Date, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Float32, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Float32, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Float64, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Float64, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Int64, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Json, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Json, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Numeric, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Numeric, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.String, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.String, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.String, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.String, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Timestamp, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p" + ++index].Code);
                        Assert.Equal(V1.TypeCode.Timestamp, types["p" + index].ArrayElementType.Code);
                        Assert.Equal(24, index);
                    }
                    else
                    {
                        // SpannerLib only includes a type code if one has explicitly been set for the parameter.
                        Assert.Equal(10, types.Count);
                        Assert.Equal(V1.TypeCode.Int64, types["p0"].Code);
                        Assert.Equal(V1.TypeCode.Bytes, types["p4"].Code);
                        Assert.Equal(V1.TypeCode.Bytes, types["p6"].Code);
                        Assert.Equal(V1.TypeCode.Date, types["p8"].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p9"].Code);
                        Assert.Equal(V1.TypeCode.Date, types["p9"].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Float32, types["p10"].Code);
                        Assert.Equal(V1.TypeCode.Float64, types["p12"].Code);
                        Assert.Equal(V1.TypeCode.Array, types["p16"].Code);
                        Assert.Equal(V1.TypeCode.Json, types["p16"].ArrayElementType.Code);
                        Assert.Equal(V1.TypeCode.Numeric, types["p17"].Code);
                        Assert.Equal(V1.TypeCode.Timestamp, types["p23"].Code);
                    }
                }
            );
            
            // Verify the parameter values.
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request =>
                {
                    // Assert.Single(request.Statements);
                    Assert.Contains("PENDING_COMMIT_TIMESTAMP()", request.Sql);
                    var fields = request.Params.Fields;
                    int index = -1;
                    Assert.Equal("1", fields["p" + ++index].StringValue);
                    Assert.Equal(Value.KindOneofCase.NullValue, fields["p" + ++index].KindCase);
                    Assert.True(fields["p" + ++index].BoolValue);
                    Assert.Collection(fields["p" + ++index].ListValue.Values,
                        v => Assert.True(v.BoolValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.False(v.BoolValue)
                    );
                    Assert.Equal(Convert.ToBase64String(new byte[]{1,2,3}), fields["p" + ++index].StringValue);
                    Assert.Collection(fields["p" + ++index].ListValue.Values,
                        v => Assert.Equal(Convert.ToBase64String(new byte[]{1,2,3}), v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal(Convert.ToBase64String(new byte[]{3,2,1}), v.StringValue)
                    );
                    Assert.Equal("", fields["p" + ++index].StringValue);
                    Assert.Empty(fields["p" + ++index].ListValue.Values);
                    Assert.Equal("2000-01-01", fields["p" + ++index].StringValue);
                    Assert.Collection(fields["p" + ++index].ListValue.Values,
                        v => Assert.Equal("2021-08-26", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("2000-01-01", v.StringValue)
                    );
                    Assert.Equal(3.14f, fields["p" + ++index].NumberValue);
                    Assert.Collection(fields["p" + ++index].ListValue.Values,
                        v => Assert.Equal(3.14f, v.NumberValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal(6.626f, v.NumberValue)
                    );
                    Assert.Equal(3.14d, fields["p" + ++index].NumberValue);
                    Assert.Collection(fields["p" + ++index].ListValue.Values,
                        v => Assert.Equal(3.14d, v.NumberValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal(6.626d, v.NumberValue)
                    );
                    Assert.Collection(fields["p" + ++index].ListValue.Values,
                        v => Assert.Equal("1", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("2", v.StringValue)
                    );
                    Assert.Equal("{\"key\": \"value\"}", fields["p" + ++index].StringValue);
                    Assert.Collection(fields["p" + ++index].ListValue.Values,
                        v => Assert.Equal("{\"key1\": \"value1\"}", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("{\"key2\": \"value2\"}", v.StringValue)
                    );
                    Assert.Equal("6.626", fields["p" + ++index].StringValue);
                    Assert.Collection(fields["p" + ++index].ListValue.Values,
                        v => Assert.Equal("3.14", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("6.626", v.StringValue)
                    );
                    Assert.Equal("test", fields["p" + ++index].StringValue);
                    Assert.Collection(fields["p" + ++index].ListValue.Values,
                        v => Assert.Equal("test1", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("test2", v.StringValue)
                    );
                    Assert.Equal("", fields["p" + ++index].StringValue);
                    Assert.Empty(fields["p" + ++index].ListValue.Values);
                    Assert.Equal("2000-01-01T00:00:00.0000000Z", fields["p" + ++index].StringValue);
                    Assert.Collection(fields["p" + ++index].ListValue.Values,
                        v => Assert.Equal("2000-01-01T00:00:00.0010000Z", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("2000-01-01T00:00:00.0020000Z", v.StringValue)
                    );
                    Assert.Equal(24, index);
                }
            );
        }

        [CombinatorialData]
        [Theory]
        public async Task CanQueryAllTypes(bool useTransaction)
        {
            var sql = "SELECT * FROM TableWithAllTypes";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, CreateTableWithAllColumnTypesResultSet());
            string connectionString = $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";
            await using var connection = new SpannerRetriableConnection(new SpannerConnection(connectionString, ChannelCredentials.Insecure));
            await using var cmd = connection.CreateSelectCommand(sql);
            SpannerRetriableTransaction transaction = null;
            if (useTransaction)
            {
                transaction = await connection.BeginTransactionAsync();
                cmd.Transaction = transaction;
            }
            await using var reader = await cmd.ExecuteReaderAsync();
            if (useTransaction)
            {
                Assert.False(((SpannerDataReaderWithChecksum)reader).SpannerDataReader.IsClosed);
            }
            while (await reader.ReadAsync())
            {
                Assert.Equal(1L, reader.GetInt64(0));
                Assert.Equal(1L, reader.GetFieldValue<long>(0));
                Assert.Equal(3.14d, reader.GetDouble(1));
                Assert.Equal(3.14d, reader.GetFieldValue<double>(1));
                Assert.Equal(SpannerNumeric.FromDecimal(3.14m, LossOfPrecisionHandling.Throw), reader.GetFieldValue<SpannerNumeric>(2));
                Assert.True(reader.GetBoolean(3));
                Assert.True(reader.GetFieldValue<bool>(3));
                Assert.Equal("test", reader.GetString(4));
                Assert.Equal("test", reader.GetFieldValue<string>(4));
                Assert.Equal(Encoding.Unicode.GetBytes("test"), reader.GetFieldValue<byte[]>(5));
                Assert.Equal(new DateTime(2021, 12, 1), reader.GetDateTime(6));
                Assert.Equal(new DateTime(2021, 12, 1), reader.GetFieldValue<DateTime>(6));
                Assert.Equal(new DateTime(2021, 12, 1, 11, 17, 2, DateTimeKind.Utc), reader.GetDateTime(7));
                Assert.Equal(new DateTime(2021, 12, 1, 11, 17, 2, DateTimeKind.Utc), reader.GetFieldValue<DateTime>(7));
                Assert.Equal("{\"key1\": \"value1\", \"key2\": \"value2\"}", reader.GetString(8));
                Assert.Equal("{\"key1\": \"value1\", \"key2\": \"value2\"}", reader.GetFieldValue<string>(8));
                Assert.Equal(new List<long?>{1L, 2L, 4L, null}, reader.GetFieldValue<List<long?>>(9));
                Assert.Equal(new List<double?>{3.14d, 6.626d, null}, reader.GetFieldValue<List<double?>>(10));
                Assert.Equal(new List<SpannerNumeric?> { SpannerNumeric.Zero, SpannerNumeric.Epsilon, SpannerNumeric.MaxValue, null}, reader.GetFieldValue<List<SpannerNumeric?>>(11));
                Assert.Equal(new List<bool?>{true, false, null}, reader.GetFieldValue<List<bool?>>(12));
                Assert.Equal(new List<string>{"test1", "test2", null}, reader.GetFieldValue<List<string>>(13));
                Assert.Equal(new List<byte[]>{Encoding.Unicode.GetBytes("test1"), Encoding.Unicode.GetBytes("test2"), null}, reader.GetFieldValue<List<byte[]>>(14));
                Assert.Equal(new List<DateTime?>{new DateTime(2021, 12, 1), new DateTime(2000, 2, 29), null}, reader.GetFieldValue<List<DateTime?>>(15));
                Assert.Equal(new List<DateTime?>{new DateTime(2021, 12, 1, 11, 17, 2, DateTimeKind.Utc), null}, reader.GetFieldValue<List<DateTime?>>(16));
                Assert.Equal(new List<string>{"{\"key1\": \"value1\", \"key2\": \"value2\"}", "{\"key1\": \"value3\", \"key2\": \"value4\"}", null}, reader.GetFieldValue<List<string>>(17));
            }
            await reader.DisposeAsync();
            if (useTransaction)
            {
                Assert.True(((SpannerDataReaderWithChecksum)reader).SpannerDataReader.IsClosed);
            }
            if (useTransaction)
            {
                await transaction.CommitAsync();
            }
        }
        
        internal static StatementResult CreateTableWithAllColumnTypesResultSet()
        {
            return StatementResult.CreateResultSet(new List<Tuple<V1.Type, string>>
            {
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Int64}, "Int64"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Float64}, "Float64"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Numeric}, "Numeric"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Bool}, "Bool"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.String}, "String"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Bytes}, "Bytes"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Date}, "Date"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Timestamp}, "Timestamp"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Json}, "Json"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Int64}}, "Int64Array"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Float64}}, "Float64Array"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Numeric}}, "NumericArray"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Bool}}, "BoolArray"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.String}}, "StringArray"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Bytes}}, "BytesArray"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Date}}, "DateArray"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Timestamp}}, "TimestampArray"),
                Tuple.Create(new V1.Type{ Code = V1.TypeCode.Array, ArrayElementType = new V1.Type {Code = V1.TypeCode.Json}}, "JsonArray"),
            }, new List<object[]>
                {
                    new object[]
                    {
                        1L,
                        3.14d,
                        SpannerNumeric.FromDecimal(3.14m, LossOfPrecisionHandling.Throw),
                        true,
                        "test",
                        Encoding.Unicode.GetBytes("test"),
                        new DateTime(2021, 12, 1),
                        new DateTime(2021, 12, 1, 11, 17, 2, DateTimeKind.Utc),
                        "{\"key1\": \"value1\", \"key2\": \"value2\"}",
                        new List<long?>{1L, 2L, 4L, null},
                        new List<double?>{3.14d, 6.626d, null},
                        new List<SpannerNumeric?> { SpannerNumeric.Zero, SpannerNumeric.Epsilon, SpannerNumeric.MaxValue, null},
                        new List<bool?>{true, false, null},
                        new List<string>{"test1", "test2", null},
                        new List<byte[]>{Encoding.Unicode.GetBytes("test1"), Encoding.Unicode.GetBytes("test2"), null},
                        new List<DateTime?>{new DateTime(2021, 12, 1), new DateTime(2000, 2, 29), null},
                        new List<DateTime?>{new DateTime(2021, 12, 1, 11, 17, 2, DateTimeKind.Utc), null},
                        new List<string>{"{\"key1\": \"value1\", \"key2\": \"value2\"}", "{\"key1\": \"value3\", \"key2\": \"value4\"}", null},
                    },
                }
            );
        }

        [SkippableFact]
        public async Task RequestIncludesEfCoreClientHeader()
        {
            Skip.IfNot(UsesClientLib());
            
            var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, " +
                      $"`s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                      $"WHERE `s`.`SingerId` = @p_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.Type, string>>(),
                new List<object[]>()));

            using var db = new MockServerSampleDbContext(ConnectionString);
            await db.Singers.FindAsync(1L);

            Assert.NotEmpty(_fixture.SpannerMock.Headers);
            Assert.All(_fixture.SpannerMock.Headers, headers =>
            {
                var entry = headers.Get("x-goog-api-client");
                Assert.NotNull(entry);
                Assert.Contains("efcore", entry.Value);
            });
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
    }
}
#pragma warning restore EF1001
