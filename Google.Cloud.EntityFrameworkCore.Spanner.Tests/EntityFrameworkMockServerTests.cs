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
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
    internal class MockServerDbContext : SpannerSampleDbContext
    {
        private readonly string _connectionString;

        internal MockServerDbContext(string connectionString) : base()
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

            using var db = new MockServerDbContext(ConnectionString);
            var singer = await db.Singers.FindAsync(1L);
            Assert.Null(singer);
        }

        [Fact]
        public async Task FindSingerAsync_ReturnsInstance_IfFound()
        {
            var sql = AddFindSingerResult();

            using var db = new MockServerDbContext(ConnectionString);
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

            using var db = new MockServerDbContext(ConnectionString);
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

            using var db = new MockServerDbContext(ConnectionString);
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

            using var db = new MockServerDbContext(ConnectionString);
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
            using var db = new MockServerDbContext(ConnectionString);
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
            using var db = new MockServerDbContext(ConnectionString);
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

        private string AddFindSingerResult()
        {
            var sql = "SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture\r\nFROM Singers AS s\r\nWHERE s.SingerId = @__p_0\r\nLIMIT 1";
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
