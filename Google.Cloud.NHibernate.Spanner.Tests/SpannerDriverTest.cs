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

using Google.Cloud.NHibernate.Spanner.Tests.Entities;
using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.Data;
using Google.Protobuf.WellKnownTypes;
using NHibernate;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Xunit;
using IsolationLevel = System.Data.IsolationLevel;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.NHibernate.Spanner.Tests
{
    public class SpannerDriverTest : IClassFixture<NHibernateMockServerFixture>
    {
        private readonly NHibernateMockServerFixture _fixture;

        public SpannerDriverTest(NHibernateMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        [Fact]
        public async Task GetSingerAsync_ReturnsNull_IfNotFound()
        {
            using var session = _fixture.SessionFactory.OpenSession();
            AddEmptySingerResult(GetSelectSingerSql());

            var singer = await session.GetAsync<Singer>(1L);
            Assert.Null(singer);
        }

        [Fact]
        public async Task GetSingerAsync_ReturnsNotNull_IfFound()
        {
            using var session = _fixture.SessionFactory.OpenSession();

            var sql = AddSingerResult(GetSelectSingerSql());
            var singer = await session.GetAsync<Singer>(1L);
            Assert.NotNull(singer);
            Assert.Equal("Alice", singer.FirstName);
            Assert.Equal(1L, singer.SingerId);
            Assert.Equal(new SpannerDate(1998, 5, 12), singer.BirthDate);
            Assert.Equal("Alice", singer.FirstName);
            Assert.Equal("Alice Morrison", singer.FullName);
            Assert.Equal("Morrison", singer.LastName);
            Assert.Null(singer.Picture);

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.Null(request.Transaction);
                }
            );
            // A read-only operation should not initiate and commit a transaction.
            Assert.Empty(_fixture.SpannerMock.Requests.OfType<V1.CommitRequest>());
        }

        [Fact]
        public async Task InsertMultipleSingers_UsesSameTransaction()
        {
            var insertSql = "INSERT INTO Singer (FirstName, LastName, BirthDate, SingerId) VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Morrison", 0);

            using var session = _fixture.SessionFactory.OpenSession();
            var transaction = session.BeginTransaction();
            
            await session.SaveAsync(new Singer
            {
                SingerId = 1L,
                FirstName = "Alice",
                LastName = "Morrison",
            });
            await session.SaveAsync(new Singer
            {
                SingerId = 2L,
                FirstName = "Alice",
                LastName = "Morrison",
            });
            await transaction.CommitAsync();

            var transactionId = _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>().First().Transaction.Id;
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(insertSql, request.Sql);
                    Assert.Equal(transactionId, request.Transaction.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.Equal(transactionId, request.Transaction.Id);
                },
                request =>
                {
                    Assert.Equal(insertSql, request.Sql);
                    Assert.Equal(transactionId, request.Transaction.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.Equal(transactionId, request.Transaction.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));
        }

        [Fact]
        public async Task InsertSinger_SelectsFullName()
        {
            using var session = _fixture.SessionFactory.OpenSession();

            var insertSql = "INSERT INTO Singer (FirstName, LastName, BirthDate, SingerId) VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Morrison", 0);

            var singer = new Singer
            {
                SingerId = 1L,
                FirstName = "Alice",
                LastName = "Morrison",
            };
            var id = await session.SaveAsync(singer);
            await session.FlushAsync();

            Assert.Equal(1L, id);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(insertSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.Null(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));

            Assert.Collection(_fixture.SpannerMock.Requests
                .Where(request => request is V1.CommitRequest || request is V1.ExecuteSqlRequest)
                .Select(request => request.GetType()),
                request => Assert.Equal(typeof(V1.ExecuteSqlRequest), request),
                request => Assert.Equal(typeof(V1.CommitRequest), request),
                request => Assert.Equal(typeof(V1.ExecuteSqlRequest), request)
            );
        }

        [Fact]
        public async Task UpdateSinger_SelectsFullName()
        {
            // Setup results.
            var updateSql = "UPDATE Singer SET FirstName = @p0, LastName = @p1, BirthDate = @p2 WHERE SingerId = @p3";
            _fixture.SpannerMock.AddOrUpdateStatementResult(updateSql, StatementResult.CreateUpdateCount(1L));
            var selectSingerSql = AddSingerResult(GetSelectSingerSql());
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Pieterson-Morrison", 0);

            using var session = _fixture.SessionFactory.OpenSession();
            var singer = await session.GetAsync<Singer>(1L);
            singer.LastName = "Pieterson-Morrison";
            await session.FlushAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(selectSingerSql, request.Sql);
                    Assert.Null(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(updateSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.Null(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));
        }

        [Fact]
        public async Task DeleteSinger_DoesNotSelectFullName()
        {
            // Setup results.
            var deleteSql = $"DELETE FROM Singer WHERE SingerId = @p0";
            _fixture.SpannerMock.AddOrUpdateStatementResult(deleteSql, StatementResult.CreateUpdateCount(1L));
            var selectSingerSql = AddSingerResult(GetSelectSingerSql());

            using var session = _fixture.SessionFactory.OpenSession();
            var singer = await session.GetAsync<Singer>(1L);
            await session.DeleteAsync(singer);
            await session.FlushAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(selectSingerSql, request.Sql);
                    Assert.Null(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(deleteSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests.Where(request => request is V1.CommitRequest));
        }

        [Fact]
        public async Task CanUseReadOnlyTransaction()
        {
            var sql = AddSingerResult(GetSelectSingerSql());
            using var session = _fixture.SessionFactory.OpenSession();
            using var connection = session.Connection as SpannerRetriableConnection;
            connection!.ReadOnlyStaleness = TimestampBound.Strong;

            var transaction = session.BeginTransaction(IsolationLevel.Snapshot);
            var singer = await session.GetAsync<Singer>(1L);
            await transaction.CommitAsync();
            
            Assert.NotNull(singer);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests
                .OfType<V1.BeginTransactionRequest>()
                .Where(request => request.Options?.ReadOnly?.Strong ?? false));
        }

        [Fact]
        public async Task CanUseReadOnlyTransactionWithTimestampBound()
        {
            var sql = AddSingerResult(GetSelectSingerSql());
            using var session = _fixture.SessionFactory.OpenSession();
            using var connection = session.Connection as SpannerRetriableConnection;
            connection!.CreateReadOnlyTransactionForSnapshot = true;
            connection.ReadOnlyStaleness = TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(10));

            var transaction = session.BeginTransaction(IsolationLevel.Snapshot);
            var singer = await session.GetAsync<Singer>(1L);
            await transaction.CommitAsync();
            
            Assert.NotNull(singer);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                }
            );
            Assert.Single(_fixture.SpannerMock.Requests
                .OfType<V1.BeginTransactionRequest>()
                .Where(request => request.Options?.ReadOnly?.ExactStaleness?.Seconds == 10L));
        }

        [Fact]
        public async Task CanReadWithMaxStaleness()
        {
            var sql = AddSingerResult(GetSelectSingerSql());
            using var session = _fixture.SessionFactory.OpenSession();
            using var connection = session.Connection as SpannerRetriableConnection;
            connection!.ReadOnlyStaleness = TimestampBound.OfMaxStaleness(TimeSpan.FromSeconds(10));
            var singer = await session.GetAsync<Singer>(1L);

            Assert.NotNull(singer);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(sql, request.Sql);
                    // Assert.Equal(Duration.FromTimeSpan(TimeSpan.FromSeconds(10)), request.Transaction?.SingleUse?.ReadOnly?.MaxStaleness);
                }
            );
        }

        [SkippableFact]
        public async Task InsertMultipleSingers_UsesBatch()
        {
            Skip.If(true, "Batching not yet implemented");
            using var session = _fixture.SessionFactory.OpenSession();

            // Setup results.
            var insertSql = "INSERT INTO Singer (FirstName, LastName, BirthDate, SingerId) VALUES (@p0, @p1, @p2, @p3)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));
            var selectFullNameSql = AddSelectSingerFullNameResult("Alice Morrison", 0);

            var tx = session.BeginTransaction();
            
            await session.SaveAsync(new Singer
            {
                SingerId = 1L,
                FirstName = "Alice",
                LastName = "Morrison",
            });
            await session.SaveAsync(new Singer
            {
                SingerId = 2L,
                FirstName = "Alice",
                LastName = "Morrison",
            });
            await tx.CommitAsync();
            await session.FlushAsync();

            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<V1.ExecuteSqlRequest>(),
                request =>
                {
                    Assert.Equal(insertSql, request.Sql);
                    Assert.NotNull(request.Transaction?.Id);
                },
                request =>
                {
                    Assert.Equal(selectFullNameSql, request.Sql);
                    Assert.Null(request.Transaction?.Id);
                }
            );
        }

        private static string GetSelectSingerSql() =>
            "SELECT singer0_.SingerId as singerid1_0_0_, singer0_.FirstName as firstname2_0_0_, singer0_.LastName as lastname3_0_0_, singer0_.FullName as fullname4_0_0_"
                + ", singer0_.BirthDate as birthdate5_0_0_"
                // + ", singer0_.Picture as picture6_0_0_"
                + " FROM Singer singer0_ WHERE singer0_.SingerId=@p0";
        
        private void AddEmptySingerResult(string sql) => AddSingerResults(sql, new List<object[]>());

        private string AddSingerResult(string sql) =>
            AddSingerResults(sql, new List<object[]>
            {
                new object[] { 1L, "Alice", "Morrison", "Alice Morrison", "1998-05-12", Convert.ToBase64String(new byte[]{1,2,3}) },
            });

        private string AddSingerResults(string sql, IEnumerable<object[]> rows)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "singerid1_0_0_"),
                    Tuple.Create(V1.TypeCode.String, "firstname2_0_0_"),
                    Tuple.Create(V1.TypeCode.String, "lastname3_0_0_"),
                    Tuple.Create(V1.TypeCode.String, "fullname4_0_0_"),
                    Tuple.Create(V1.TypeCode.Date, "birthdate5_0_0_"),
                    Tuple.Create(V1.TypeCode.Bytes, "picture6_0_0_"),
                }, rows));
            return sql;
        }

        private string AddSelectSingerFullNameResult(string fullName, int paramIndex)
        {
            var selectFullNameSql =
                $"SELECT singer_.FullName as fullname4_0_ FROM Singer singer_ WHERE singer_.SingerId=@p{paramIndex}";
            _fixture.SpannerMock.AddOrUpdateStatementResult(selectFullNameSql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "fullname4_0_"),
                },
                new List<object[]>
                {
                    new object[] { fullName },
                }
            ));
            return selectFullNameSql;
        }
    }
}