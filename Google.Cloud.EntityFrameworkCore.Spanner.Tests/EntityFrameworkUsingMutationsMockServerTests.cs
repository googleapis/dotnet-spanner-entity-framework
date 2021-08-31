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
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
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
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{

    internal class MockServerSampleDbContextUsingMutations : SpannerSampleDbContext
    {
        private readonly string _connectionString;

        internal MockServerSampleDbContextUsingMutations(string connectionString) : base()
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                    .UseMutations(MutationUsage.Always)
                    .UseLazyLoadingProxies();
            }
        }
    }

    internal class MockServerVersionDbContextUsingMutations : SpannerVersionDbContext
    {
        private readonly string _connectionString;

        internal MockServerVersionDbContextUsingMutations(string connectionString) : base()
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                    .UseMutations(MutationUsage.Always)
                    .UseLazyLoadingProxies();
            }
        }
    }

    /// <summary>
    /// Tests CRUD operations using mutations on an in-mem Spanner mock server.
    /// </summary>
    public class EntityFrameworkMockUsingMutationsServerTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public EntityFrameworkMockUsingMutationsServerTests(SpannerMockServerFixture service)
        {
            _fixture = service;
            service.SpannerMock.Reset();
        }

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        [Fact]
        public async Task InsertAlbum()
        {
            using var db = new MockServerSampleDbContextUsingMutations(ConnectionString);
            db.Albums.Add(new Albums
            {
                AlbumId = 1L,
                Title = "Some title",
                SingerId = 1L,
                ReleaseDate = new SpannerDate(2000, 1, 1),
            });
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is CommitRequest).Select(request => (CommitRequest)request),
                request => {
                    Assert.Single(request.Mutations);
                    var mutation = request.Mutations[0];
                    Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
                    Assert.Equal("Albums", mutation.Insert.Table);
                    var row = mutation.Insert.Values[0];
                    var cols = mutation.Insert.Columns;
                    Assert.Equal("1", row.Values[cols.IndexOf("AlbumId")].StringValue);
                    Assert.Equal("Some title", row.Values[cols.IndexOf("Title")].StringValue);
                    Assert.Equal("1", row.Values[cols.IndexOf("SingerId")].StringValue);
                    Assert.Equal("2000-01-01", row.Values[cols.IndexOf("ReleaseDate")].StringValue);
                }
            );
        }

        [Fact]
        public async Task InsertSinger()
        {
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Morrison", 0);

            using var db = new MockServerSampleDbContextUsingMutations(ConnectionString);
            db.Singers.Add(new Singers
            {
                SingerId = 1L,
                FirstName = "Alice",
                LastName = "Morrison",
            });
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is CommitRequest).Select(request => (CommitRequest)request),
                request => {
                    Assert.Single(request.Mutations);
                    var mutation = request.Mutations[0];
                    Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
                    Assert.Equal("Singers", mutation.Insert.Table);
                    var row = mutation.Insert.Values[0];
                    var cols = mutation.Insert.Columns;
                    Assert.Equal("1", row.Values[cols.IndexOf("SingerId")].StringValue);
                    Assert.Equal("Alice", row.Values[cols.IndexOf("FirstName")].StringValue);
                    Assert.Equal("Morrison", row.Values[cols.IndexOf("LastName")].StringValue);
                    Assert.Equal(-1, cols.IndexOf("FullName"));
                }
            );
            // Verify that the SELECT for the FullName is done after the commit.
            Assert.Collection(_fixture.SpannerMock.Requests
                .Where(request => request is CommitRequest || request is ExecuteSqlRequest)
                .Select(request => request.GetType()),
                request => Assert.Equal(typeof(CommitRequest), request),
                request => Assert.Equal(typeof(ExecuteSqlRequest), request));
            Assert.Single(_fixture.SpannerMock.Requests
                .Where(request => request is ExecuteSqlRequest sqlRequest && sqlRequest.Sql.Trim() == selectFullNameSql.Trim()));
        }

        [Fact]
        public async Task InsertSingerInTransaction()
        {
            using var db = new MockServerSampleDbContextUsingMutations(ConnectionString);
            using var transaction = await db.Database.BeginTransactionAsync();
            db.Singers.Add(new Singers
            {
                SingerId = 1L,
                FirstName = "Alice",
                LastName = "Morrison",
            });
            var updateCount = await db.SaveChangesAsync();
            await transaction.CommitAsync();

            Assert.Equal(1L, updateCount);
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is CommitRequest).Select(request => (CommitRequest)request),
                request => {
                    Assert.Single(request.Mutations);
                    var mutation = request.Mutations[0];
                    Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
                    Assert.Equal("Singers", mutation.Insert.Table);
                    var row = mutation.Insert.Values[0];
                    var cols = mutation.Insert.Columns;
                    Assert.Equal("1", row.Values[cols.IndexOf("SingerId")].StringValue);
                    Assert.Equal("Alice", row.Values[cols.IndexOf("FirstName")].StringValue);
                    Assert.Equal("Morrison", row.Values[cols.IndexOf("LastName")].StringValue);
                    Assert.Equal(-1, cols.IndexOf("FullName"));
                }
            );
            // Verify that EF Core does NOT try to fetch the name of the Singer, even though it is a computed
            // column that should normally be propagated. The fetch is skipped because the update uses mutations
            // in combination with manual transactions. Trying to fetch the name of the singer is therefore not
            // possible during the transaction.
            Assert.Collection(_fixture.SpannerMock.Requests
                .Where(request => request is CommitRequest || request is ExecuteSqlRequest)
                .Select(request => request.GetType()),
                request => Assert.Equal(typeof(CommitRequest), request));
        }

        [Fact]
        public async Task UpdateSinger_SelectsFullName()
        {
            // Setup results.
            var selectSingerSql = AddFindSingerResult($"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, " +
                $"`s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` = @__p_0{Environment.NewLine}LIMIT 1");
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Pieterson-Morrison", 0);

            using var db = new MockServerSampleDbContextUsingMutations(ConnectionString);
            var singer = await db.Singers.FindAsync(1L);
            singer.LastName = "Pieterson-Morrison";
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            Assert.Collection(
                _fixture.SpannerMock.Requests
                    .Where(request => !new[] { typeof(BeginTransactionRequest), typeof(BatchCreateSessionsRequest) }.Contains(request.GetType()))
                    .Select(request => request.GetType()),
                request => Assert.Equal(typeof(ExecuteSqlRequest), request),
                request => Assert.Equal(typeof(CommitRequest), request),
                request => Assert.Equal(typeof(ExecuteSqlRequest), request)
            );
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest).Select(request => (ExecuteSqlRequest)request),
                request =>
                {
                    Assert.Equal(selectSingerSql.Trim(), request.Sql.Trim());
                    Assert.Null(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql.Trim(), request.Sql.Trim());
                    Assert.Null(request.Transaction?.Id);
                }
            );
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is CommitRequest).Select(request => request as CommitRequest),
                request =>
                {
                    Assert.Collection(
                        request.Mutations,
                        mutation =>
                        {
                            Assert.Equal(Mutation.OperationOneofCase.Update, mutation.OperationCase);
                            Assert.Equal("Singers", mutation.Update.Table);
                            Assert.Collection(
                                mutation.Update.Columns,
                                column => Assert.Equal("SingerId", column),
                                column => Assert.Equal("LastName", column)
                            );
                            Assert.Collection(
                                mutation.Update.Values,
                                row =>
                                {
                                    Assert.Collection(
                                        row.Values,
                                        value => Assert.Equal("1", value.StringValue),
                                        value => Assert.Equal("Pieterson-Morrison", value.StringValue)
                                    );
                                }
                            );
                        }
                    );
                }
            );
        }

        [Fact]
        public async Task DeleteSinger_DoesNotSelectFullName()
        {
            using var db = new MockServerSampleDbContextUsingMutations(ConnectionString);
            db.Singers.Remove(new Singers { SingerId = 1L });
            var updateCount = await db.SaveChangesAsync();

            Assert.Equal(1L, updateCount);
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is CommitRequest).Select(request => (CommitRequest)request),
                request =>
                {
                    Assert.Single(request.Mutations);
                    var mutation = request.Mutations[0];
                    Assert.Equal(Mutation.OperationOneofCase.Delete, mutation.OperationCase);
                    Assert.Equal("Singers", mutation.Delete.Table);
                    var keySet = mutation.Delete.KeySet;
                    Assert.False(keySet.All);
                    Assert.Empty(keySet.Ranges);
                    Assert.Single(keySet.Keys);
                    Assert.Single(keySet.Keys[0].Values);
                    Assert.Equal("1", keySet.Keys[0].Values[0].StringValue);
                }
            );
            Assert.Empty(_fixture.SpannerMock.Requests.Where(request => request is ExecuteBatchDmlRequest));
            Assert.Empty(_fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest));
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is CommitRequest));
        }

        [Fact]
        public async Task VersionNumberIsAutomaticallyGeneratedOnInsertAndUpdate()
        {
            using var db = new MockServerVersionDbContextUsingMutations(ConnectionString);
            var singer = new SingersWithVersion { SingerId = 1L, FirstName = "Pete", LastName = "Allison" };
            db.Singers.Add(singer);
            await db.SaveChangesAsync();

            Assert.Empty(_fixture.SpannerMock.Requests.Where(r => r is ExecuteBatchDmlRequest));
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(r => r is CommitRequest).Select(r => r as CommitRequest),
                r =>
                {
                    Assert.Collection(
                        r.Mutations,
                        mutation =>
                        {
                            Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
                            Assert.Equal("SingersWithVersion", mutation.Insert.Table);
                            Assert.Collection(
                                mutation.Insert.Columns,
                                column => Assert.Equal("SingerId", column),
                                column => Assert.Equal("FirstName", column),
                                column => Assert.Equal("LastName", column),
                                column => Assert.Equal("Version", column)
                            );
                            Assert.Collection(
                                mutation.Insert.Values,
                                row => Assert.Collection(
                                    row.Values,
                                    value => Assert.Equal("1", value.StringValue),
                                    value => Assert.Equal("Pete", value.StringValue),
                                    value => Assert.Equal("Allison", value.StringValue),
                                    value => Assert.Equal("1", value.StringValue)
                                )
                            );
                        }
                    );
                }
            );

            _fixture.SpannerMock.Reset();
            // Update the singer and verify that the version number is first checked using a SELECT statement and then is updated in a mutation.
            var concurrencySql = $"SELECT 1 FROM `SingersWithVersion` {Environment.NewLine}WHERE `SingerId` = @p0 AND `Version` = @p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(concurrencySql, StatementResult.CreateSelect1ResultSet());
            singer.LastName = "Peterson - Allison";
            await db.SaveChangesAsync();

            Assert.Empty(_fixture.SpannerMock.Requests.Where(r => r is ExecuteBatchDmlRequest));
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(r => r is ExecuteSqlRequest).Select(r => r as ExecuteSqlRequest),
                r =>
                {
                    Assert.Equal("1", r.Params.Fields["p0"].StringValue); // SingerId
                    Assert.Equal("1", r.Params.Fields["p1"].StringValue); // Version
                }
            );
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(r => r is CommitRequest).Select(r => r as CommitRequest),
                r =>
                {
                    Assert.Collection(
                        r.Mutations,
                        mutation =>
                        {
                            Assert.Equal(Mutation.OperationOneofCase.Update, mutation.OperationCase);
                            Assert.Equal("SingersWithVersion", mutation.Update.Table);
                            Assert.Collection(
                                mutation.Update.Columns,
                                column => Assert.Equal("SingerId", column),
                                column => Assert.Equal("LastName", column),
                                column => Assert.Equal("Version", column)
                            );
                            Assert.Collection(
                                mutation.Update.Values,
                                row => Assert.Collection(
                                    row.Values,
                                    value => Assert.Equal("1", value.StringValue),
                                    value => Assert.Equal("Peterson - Allison", value.StringValue),
                                    value => Assert.Equal("2", value.StringValue)
                                )
                            );
                        }
                    );
                }
            );
        }

        [Fact]
        public async Task UpdateFailsIfVersionNumberChanged()
        {
            using var db = new MockServerVersionDbContextUsingMutations(ConnectionString);

            // Set the result of the concurrency check to an empty result set to simulate a version number that has changed.
            var concurrencySql = $"SELECT 1 FROM `SingersWithVersion` {Environment.NewLine}WHERE `SingerId` = @p0 AND `Version` = @p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(concurrencySql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "COL1"));

            // Attach a singer to the context and try to update it.
            var singer = new SingersWithVersion { SingerId = 1L, FirstName = "Pete", LastName = "Allison", Version = 1L };
            db.Attach(singer);

            singer.LastName = "Allison - Peterson";
            await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync());

            // Update the concurrency check result to 1 to simulate a resolved version conflict.
            _fixture.SpannerMock.AddOrUpdateStatementResult(concurrencySql, StatementResult.CreateSelect1ResultSet());
            Assert.Equal(1L, await db.SaveChangesAsync());
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task ExplicitAndImplicitTransactionIsRetried(bool disableInternalRetries, bool useExplicitTransaction)
        {
            using var db = new MockServerSampleDbContextUsingMutations(ConnectionString);
            IDbContextTransaction transaction = null;
            if (useExplicitTransaction)
            {
                // Note that using explicit transactions in combination with mutations has a couple of side-effects:
                // 1. Read-your-writes does not work.
                // 2. Computed columns are not propagated to the current context.
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

            // Abort the next statement that is executed on the mock server.
            _fixture.SpannerMock.AbortNextStatement();
            // We can only disable internal retries when using explicit transactions. Otherwise internal retries
            // are always used.
            if (disableInternalRetries && useExplicitTransaction)
            {
                await db.SaveChangesAsync();
                var e = await Assert.ThrowsAsync<SpannerException>(() => transaction.CommitAsync());
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
                Assert.Empty(_fixture.SpannerMock.Requests.Where(request => request is ExecuteBatchDmlRequest));
                Assert.Collection(
                    _fixture.SpannerMock.Requests.Where(request => request is CommitRequest).Select(request => (CommitRequest)request),
                    // The commit request is sent twice to the server, as the statement is aborted during the first attempt.
                    request =>
                    {
                        Assert.Single(request.Mutations);
                        Assert.Equal("Venues", request.Mutations.First().Insert.Table);
                        Assert.NotNull(request.TransactionId);
                    },
                    request =>
                    {
                        Assert.Single(request.Mutations);
                        Assert.Equal("Venues", request.Mutations.First().Insert.Table);
                        Assert.NotNull(request.TransactionId);
                    }
                );
            }
        }

        [Fact]
        public async Task CanInsertCommitTimestamp()
        {
            using var db = new MockServerSampleDbContextUsingMutations(ConnectionString);
            _fixture.SpannerMock.AddOrUpdateStatementResult($"{Environment.NewLine}SELECT `ColComputed`" +
                $"{Environment.NewLine}FROM `TableWithAllColumnTypes`{Environment.NewLine}WHERE  TRUE  AND `ColInt64` = @p0", StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.String }, "FOO"));

            db.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes { ColInt64 = 1L });
            await db.SaveChangesAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is CommitRequest).Select(request => (CommitRequest)request),
                request =>
                {
                    Assert.Single(request.Mutations);
                    var mutation = request.Mutations[0];
                    Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
                    Assert.Single(mutation.Insert.Values);
                    var row = mutation.Insert.Values[0];
                    var cols = mutation.Insert.Columns;
                    Assert.Equal("spanner.commit_timestamp()", row.Values[cols.IndexOf("ColCommitTS")].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanUpdateCommitTimestamp()
        {
            using var db = new MockServerSampleDbContextUsingMutations(ConnectionString);
            _fixture.SpannerMock.AddOrUpdateStatementResult($"{Environment.NewLine}SELECT `ColComputed`{Environment.NewLine}FROM `TableWithAllColumnTypes`{Environment.NewLine}WHERE  TRUE  AND `ColInt64` = @p0", StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.String }, "FOO"));

            var row = new TableWithAllColumnTypes { ColInt64 = 1L };
            db.TableWithAllColumnTypes.Attach(row);
            row.ColBool = true;
            await db.SaveChangesAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is CommitRequest).Select(request => (CommitRequest)request),
                request =>
                {
                    Assert.Single(request.Mutations);
                    var mutation = request.Mutations[0];
                    Assert.Equal(Mutation.OperationOneofCase.Update, mutation.OperationCase);
                    Assert.Single(mutation.Update.Values);
                    var row = mutation.Update.Values[0];
                    var cols = mutation.Update.Columns;
                    Assert.Equal("spanner.commit_timestamp()", row.Values[cols.IndexOf("ColCommitTS")].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanInsertRowWithCommitTimestampAndComputedColumn()
        {
            using var db = new MockServerSampleDbContextUsingMutations(ConnectionString);
            var selectSql = $"{Environment.NewLine}SELECT `ColComputed`{Environment.NewLine}FROM `TableWithAllColumnTypes`{Environment.NewLine}WHERE  TRUE  AND `ColInt64` = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectSql, StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.String }, "FOO"));

            db.TableWithAllColumnTypes.Add(
                new TableWithAllColumnTypes { ColInt64 = 1L }
            );
            await db.SaveChangesAsync();

            Assert.Empty(_fixture.SpannerMock.Requests.Where(request => request is ExecuteBatchDmlRequest));
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is ExecuteSqlRequest).Select(request => (ExecuteSqlRequest)request),
                request => Assert.Equal(selectSql.Trim(), request.Sql.Trim())
            );
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is CommitRequest));
            // Verify the order of the requests (that is, the Select statement should be outside the implicit transaction).
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is CommitRequest || request is ExecuteSqlRequest).Select(request => request.GetType()),
                requestType => Assert.Equal(typeof(CommitRequest), requestType),
                requestType => Assert.Equal(typeof(ExecuteSqlRequest), requestType)
            );
            Assert.Collection(
                _fixture.SpannerMock.Requests.Where(request => request is CommitRequest).Select(request => (CommitRequest)request),
                request =>
                {
                    Assert.Single(request.Mutations);
                    var mutation = request.Mutations[0];
                    Assert.Equal(Mutation.OperationOneofCase.Insert, mutation.OperationCase);
                    Assert.Single(mutation.Insert.Values);
                    var row = mutation.Insert.Values[0];
                    var cols = mutation.Insert.Columns;
                    Assert.Equal("spanner.commit_timestamp()", row.Values[cols.IndexOf("ColCommitTS")].StringValue);
                }
            );
        }

        [Fact]
        public async Task CanInsertAllTypes()
        {
            using var db = new MockServerSampleDbContextUsingMutations(ConnectionString);
            _fixture.SpannerMock.AddOrUpdateStatementResult($"{Environment.NewLine}SELECT `ColComputed`" +
                                                            $"{Environment.NewLine}FROM `TableWithAllColumnTypes`{Environment.NewLine}WHERE  TRUE  AND `ColInt64` = @p0", StatementResult.CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.String }, "FOO"));

            db.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes
            {
                ColInt64 = 1L,
                ColBool = true,
                ColBytes = new byte[] {1,2,3},
                ColDate = new SpannerDate(2000, 1, 1),
                ColFloat64 = 3.14,
                ColJson = JsonDocument.Parse("{\"key\": \"value\"}"),
                ColNumeric = SpannerNumeric.Parse("6.626"),
                ColString = "test",
                ColTimestamp = new DateTime(2000, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc),
                ColBoolArray = new List<bool?>{true, null, false},
                ColBytesArray = new List<byte[]>{new byte[]{1,2,3}, null, new byte[]{3,2,1}},
                ColBytesMax = new byte[] {},
                ColDateArray = new List<SpannerDate?>{new SpannerDate(2021, 8, 26), null, new SpannerDate(2000, 1, 1)},
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
            
            // Verify the value types.
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<CommitRequest>(),
                request =>
                {
                    var values = request.Mutations[0].Insert.Values[0].Values;
                    var columns = request.Mutations[0].Insert.Columns;
                    var index = 0;
                    
                    Assert.Equal("ColCommitTS", columns[index]);
                    Assert.Equal(Value.KindOneofCase.StringValue, values[index].KindCase);
                    Assert.Equal("spanner.commit_timestamp()", values[index++].StringValue);
                    Assert.Equal("ColInt64", columns[index]);
                    Assert.Equal(Value.KindOneofCase.StringValue, values[index].KindCase);
                    Assert.Equal("1", values[index++].StringValue);
                    Assert.Equal("ASC", columns[index]);
                    Assert.Equal(Value.KindOneofCase.NullValue, values[index++].KindCase);
                    Assert.Equal("ColBool", columns[index]);
                    Assert.Equal(Value.KindOneofCase.BoolValue, values[index].KindCase);
                    Assert.True(values[index++].BoolValue);
                    Assert.Equal("ColBoolArray", columns[index]);
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Collection(values[index++].ListValue.Values,
                        v => Assert.True(v.BoolValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.False(v.BoolValue)
                    );
                    Assert.Equal("ColBytes", columns[index]);
                    Assert.Equal(Value.KindOneofCase.StringValue, values[index].KindCase);
                    Assert.Equal(Convert.ToBase64String(new byte[]{1,2,3}), values[index++].StringValue);
                    Assert.Equal("ColBytesArray", columns[index]);
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Collection(values[index++].ListValue.Values,
                        v => Assert.Equal(Convert.ToBase64String(new byte[]{1,2,3}), v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal(Convert.ToBase64String(new byte[]{3,2,1}), v.StringValue)
                    );
                    Assert.Equal("ColBytesMax", columns[index]);
                    Assert.Equal(Value.KindOneofCase.StringValue, values[index].KindCase);
                    Assert.Equal("", values[index++].StringValue);
                    Assert.Equal("ColBytesMaxArray", columns[index]);
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Empty(values[index++].ListValue.Values);
                    Assert.Equal("ColDate", columns[index]);
                    Assert.Equal(Value.KindOneofCase.StringValue, values[index].KindCase);
                    Assert.Equal("2000-01-01", values[index++].StringValue);
                    Assert.Equal("ColDateArray", columns[index]);
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Collection(values[index++].ListValue.Values,
                        v => Assert.Equal("2021-08-26", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("2000-01-01", v.StringValue)
                    );
                    Assert.Equal("ColFloat64", columns[index]);
                    Assert.Equal(Value.KindOneofCase.NumberValue, values[index].KindCase);
                    Assert.Equal(3.14d, values[index++].NumberValue);
                    Assert.Equal("ColFloat64Array", columns[index]);
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Collection(values[index++].ListValue.Values,
                        v => Assert.Equal(3.14d, v.NumberValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal(6.626d, v.NumberValue)
                    );
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Collection(values[index++].ListValue.Values,
                        v => Assert.Equal("1", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("2", v.StringValue)
                    );
                    Assert.Equal("ColJson", columns[index]);
                    Assert.Equal(Value.KindOneofCase.StringValue, values[index].KindCase);
                    Assert.Equal("{\"key\": \"value\"}", values[index++].StringValue);
                    Assert.Equal("ColJsonArray", columns[index]);
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Collection(values[index++].ListValue.Values,
                        v => Assert.Equal("{\"key1\": \"value1\"}", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("{\"key2\": \"value2\"}", v.StringValue)
                    );
                    Assert.Equal("ColNumeric", columns[index]);
                    Assert.Equal(Value.KindOneofCase.StringValue, values[index].KindCase);
                    Assert.Equal("6.626", values[index++].StringValue);
                    Assert.Equal("ColNumericArray", columns[index]);
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Collection(values[index++].ListValue.Values,
                        v => Assert.Equal("3.14", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("6.626", v.StringValue)
                    );
                    Assert.Equal("ColString", columns[index]);
                    Assert.Equal(Value.KindOneofCase.StringValue, values[index].KindCase);
                    Assert.Equal("test", values[index++].StringValue);
                    Assert.Equal("ColStringArray", columns[index]);
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Collection(values[index++].ListValue.Values,
                        v => Assert.Equal("test1", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("test2", v.StringValue)
                    );
                    Assert.Equal("ColStringMax", columns[index]);
                    Assert.Equal(Value.KindOneofCase.StringValue, values[index].KindCase);
                    Assert.Equal("", values[index++].StringValue);
                    Assert.Equal("ColStringMaxArray", columns[index]);
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Empty(values[index++].ListValue.Values);
                    Assert.Equal("ColTimestamp", columns[index]);
                    Assert.Equal(Value.KindOneofCase.StringValue, values[index].KindCase);
                    Assert.Equal("2000-01-01T00:00:00Z", values[index++].StringValue);
                    Assert.Equal("ColTimestampArray", columns[index]);
                    Assert.Equal(Value.KindOneofCase.ListValue, values[index].KindCase);
                    Assert.Collection(values[index++].ListValue.Values,
                        v => Assert.Equal("2000-01-01T00:00:00.001Z", v.StringValue),
                        v => Assert.Equal(Value.KindOneofCase.NullValue, v.KindCase),
                        v => Assert.Equal("2000-01-01T00:00:00.002Z", v.StringValue)
                    );
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
            return selectFullNameSql;
        }
    }
}
