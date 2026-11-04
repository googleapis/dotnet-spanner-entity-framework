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

using Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Xunit;
using SpannerTypeCode = Google.Cloud.Spanner.V1.TypeCode;

#pragma warning disable EF1001

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
    public class SetOperationsAndBulkUpdateDeleteMockServerTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public SetOperationsAndBulkUpdateDeleteMockServerTests(SpannerMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        private static StatementResult CreateSingersResultSet(params (long SingerId, string FirstName, string LastName)[] singers)
        {
            var columns = new List<Tuple<SpannerTypeCode, string>>
            {
                Tuple.Create(SpannerTypeCode.Int64, "SingerId"),
                Tuple.Create(SpannerTypeCode.Date, "BirthDate"),
                Tuple.Create(SpannerTypeCode.String, "FirstName"),
                Tuple.Create(SpannerTypeCode.String, "FullName"),
                Tuple.Create(SpannerTypeCode.String, "LastName"),
                Tuple.Create(SpannerTypeCode.Bytes, "Picture"),
            };
            var rows = singers.Select(singer => new object[]
            {
                singer.SingerId.ToString(),
                null,
                singer.FirstName,
                $"{singer.FirstName} {singer.LastName}",
                singer.LastName,
                null
            }).ToList();
            return StatementResult.CreateResultSet(columns, rows);
        }

        private class TestSpannerQuerySqlGenerator : SpannerQuerySqlGenerator
        {
            public TestSpannerQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies) : base(dependencies)
            {
            }

            public string GenerateBinaryOperator(SqlBinaryExpression binaryExpression) => GetOperator(binaryExpression);

            public void GenerateSetOperationDirect(SetOperationBase setOperation) => GenerateSetOperation(setOperation);
        }

        private class UnknownSetOperation : SetOperationBase
        {
            public UnknownSetOperation(string alias, SelectExpression source1, SelectExpression source2, bool distinct)
                : base(alias, source1, source2, distinct)
            {
            }

            public override SetOperationBase Update(SelectExpression source1, SelectExpression source2) => throw new NotImplementedException();
            public override TableExpressionBase WithAlias(string newAlias) => throw new NotImplementedException();
            public override TableExpressionBase Clone(string alias, ExpressionVisitor cloningExpressionVisitor) => throw new NotImplementedException();
            public override TableExpressionBase Quote() => throw new NotImplementedException();
            protected override void Print(ExpressionPrinter expressionPrinter) => throw new NotImplementedException();
            protected override TableExpressionBase WithAnnotations(IReadOnlyDictionary<string, IAnnotation> annotations) => throw new NotImplementedException();
        }

        #region Set Operations Happy Path Tests

        [Fact]
        public async Task Union_GeneratesUnionDistinct_AndMaterializesResults()
        {
            var expectedSql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s`{Environment.NewLine}" +
                              $"UNION DISTINCT{Environment.NewLine}" +
                              $"SELECT `s0`.`SingerId`, `s0`.`BirthDate`, `s0`.`FirstName`, `s0`.`FullName`, `s0`.`LastName`, `s0`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s0`";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, CreateSingersResultSet(
                (1L, "Alice", "Singer"),
                (2L, "Bob", "Singer")));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var query = database.Singers.Union(database.Singers);
            Assert.Equal(expectedSql, query.ToQueryString());

            var results = await query.ToListAsync(TestContext.Current.CancellationToken);
            Assert.Equal(2, results.Count);
            Assert.Equal(1L, results[0].SingerId);
            Assert.Equal("Alice", results[0].FirstName);
            Assert.Equal(2L, results[1].SingerId);
            Assert.Equal("Bob", results[1].FirstName);

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
        }

        [Fact]
        public async Task Concat_GeneratesUnionAll_AndMaterializesResults()
        {
            var expectedSql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s`{Environment.NewLine}" +
                              $"UNION ALL{Environment.NewLine}" +
                              $"SELECT `s0`.`SingerId`, `s0`.`BirthDate`, `s0`.`FirstName`, `s0`.`FullName`, `s0`.`LastName`, `s0`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s0`";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, CreateSingersResultSet(
                (1L, "Charlie", "Singer"),
                (1L, "Charlie", "Singer")));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var query = database.Singers.Concat(database.Singers);
            Assert.Equal(expectedSql, query.ToQueryString());

            var results = await query.ToListAsync(TestContext.Current.CancellationToken);
            Assert.Equal(2, results.Count);
            Assert.All(results, singer => Assert.Equal("Charlie", singer.FirstName));

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
        }

        [Fact]
        public async Task Intersect_GeneratesIntersectDistinct_AndMaterializesResults()
        {
            var expectedSql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s`{Environment.NewLine}" +
                              $"INTERSECT DISTINCT{Environment.NewLine}" +
                              $"SELECT `s0`.`SingerId`, `s0`.`BirthDate`, `s0`.`FirstName`, `s0`.`FullName`, `s0`.`LastName`, `s0`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s0`";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, CreateSingersResultSet(
                (1L, "David", "Singer")));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var query = database.Singers.Intersect(database.Singers);
            Assert.Equal(expectedSql, query.ToQueryString());

            var results = await query.ToListAsync(TestContext.Current.CancellationToken);
            var matchedSinger = Assert.Single(results);
            Assert.Equal(1L, matchedSinger.SingerId);
            Assert.Equal("David", matchedSinger.FirstName);

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
        }

        [Fact]
        public async Task Except_GeneratesExceptDistinct_AndMaterializesResults()
        {
            var expectedSql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s`{Environment.NewLine}" +
                              $"EXCEPT DISTINCT{Environment.NewLine}" +
                              $"SELECT `s0`.`SingerId`, `s0`.`BirthDate`, `s0`.`FirstName`, `s0`.`FullName`, `s0`.`LastName`, `s0`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s0`";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, CreateSingersResultSet(
                (1L, "Frank", "Singer")));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var query = database.Singers.Except(database.Singers);
            Assert.Equal(expectedSql, query.ToQueryString());

            var results = await query.ToListAsync(TestContext.Current.CancellationToken);
            var remainingSinger = Assert.Single(results);
            Assert.Equal(1L, remainingSinger.SingerId);
            Assert.Equal("Frank", remainingSinger.FirstName);

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
        }

        #endregion

        #region Set Operation Topologies Tests

        [Fact]
        public async Task ChainedUnion_GeneratesMultipleUnionDistinct()
        {
            var expectedSql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s`{Environment.NewLine}" +
                              $"UNION DISTINCT{Environment.NewLine}" +
                              $"SELECT `s0`.`SingerId`, `s0`.`BirthDate`, `s0`.`FirstName`, `s0`.`FullName`, `s0`.`LastName`, `s0`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s0`{Environment.NewLine}" +
                              $"UNION DISTINCT{Environment.NewLine}" +
                              $"SELECT `s1`.`SingerId`, `s1`.`BirthDate`, `s1`.`FirstName`, `s1`.`FullName`, `s1`.`LastName`, `s1`.`Picture`{Environment.NewLine}" +
                              $"FROM `Singers` AS `s1`";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, CreateSingersResultSet((1L, "Alice", "Singer")));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var query = database.Singers.Union(database.Singers).Union(database.Singers);
            Assert.Equal(expectedSql, query.ToQueryString());

            var results = await query.ToListAsync(TestContext.Current.CancellationToken);
            Assert.Single(results);
        }

        [Fact]
        public void MixedSetOperations_UnionAndIntersect_GeneratesCorrectSql()
        {
            using var database = new MockServerSampleDbContext(ConnectionString);
            var query = database.Singers.Union(database.Singers.Intersect(database.Singers));
            var queryString = query.ToQueryString();
            var expectedSql =
                $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                $"FROM `Singers` AS `s`{Environment.NewLine}" +
                $"UNION DISTINCT{Environment.NewLine}" +
                $"({Environment.NewLine}" +
                $"    SELECT `s0`.`SingerId`, `s0`.`BirthDate`, `s0`.`FirstName`, `s0`.`FullName`, `s0`.`LastName`, `s0`.`Picture`{Environment.NewLine}" +
                $"    FROM `Singers` AS `s0`{Environment.NewLine}" +
                $"    INTERSECT DISTINCT{Environment.NewLine}" +
                $"    SELECT `s1`.`SingerId`, `s1`.`BirthDate`, `s1`.`FirstName`, `s1`.`FullName`, `s1`.`LastName`, `s1`.`Picture`{Environment.NewLine}" +
                $"    FROM `Singers` AS `s1`{Environment.NewLine}" +
                $")";
            Assert.Equal(expectedSql, queryString);
        }

        [Fact]
        public void SetOperations_WithPagingAndOrdering_GeneratesCorrectSql()
        {
            using var database = new MockServerSampleDbContext(ConnectionString);
            var query = database.Singers.OrderBy(singer => singer.LastName).Take(5)
                .Union(database.Singers.OrderBy(singer => singer.LastName).Take(10));
            var queryString = query.ToQueryString();
            var expectedSql =
                $"-- @p='5' (DbType = Int64){Environment.NewLine}" +
                $"-- @p1='10' (DbType = Int64){Environment.NewLine}" +
                $"SELECT `s1`.`SingerId`, `s1`.`BirthDate`, `s1`.`FirstName`, `s1`.`FullName`, `s1`.`LastName`, `s1`.`Picture`{Environment.NewLine}" +
                $"FROM ({Environment.NewLine}" +
                $"    SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                $"    FROM `Singers` AS `s`{Environment.NewLine}" +
                $"    ORDER BY `s`.`LastName`{Environment.NewLine}" +
                $"    LIMIT @p{Environment.NewLine}" +
                $") AS `s1`{Environment.NewLine}" +
                $"UNION DISTINCT{Environment.NewLine}" +
                $"SELECT `s2`.`SingerId`, `s2`.`BirthDate`, `s2`.`FirstName`, `s2`.`FullName`, `s2`.`LastName`, `s2`.`Picture`{Environment.NewLine}" +
                $"FROM ({Environment.NewLine}" +
                $"    SELECT `s0`.`SingerId`, `s0`.`BirthDate`, `s0`.`FirstName`, `s0`.`FullName`, `s0`.`LastName`, `s0`.`Picture`{Environment.NewLine}" +
                $"    FROM `Singers` AS `s0`{Environment.NewLine}" +
                $"    ORDER BY `s0`.`LastName`{Environment.NewLine}" +
                $"    LIMIT @p1{Environment.NewLine}" +
                $") AS `s2`";
            Assert.Equal(expectedSql, queryString);
        }

        [Fact]
        public void SetOperations_WithProjectionsAndFilters_GeneratesCorrectSql()
        {
            using var database = new MockServerSampleDbContext(ConnectionString);
            var query = database.Singers.Where(singer => singer.SingerId > 10).Select(singer => singer.FirstName)
                .Union(database.Singers.Where(singer => singer.SingerId < 5).Select(singer => singer.FirstName));
            var queryString = query.ToQueryString();
            var expectedSql =
                $"SELECT `s`.`FirstName`{Environment.NewLine}" +
                $"FROM `Singers` AS `s`{Environment.NewLine}" +
                $"WHERE `s`.`SingerId` > 10{Environment.NewLine}" +
                $"UNION DISTINCT{Environment.NewLine}" +
                $"SELECT `s0`.`FirstName`{Environment.NewLine}" +
                $"FROM `Singers` AS `s0`{Environment.NewLine}" +
                $"WHERE `s0`.`SingerId` < 5";
            Assert.Equal(expectedSql, queryString);
        }

        #endregion

        #region Bulk Delete Tests

        [Fact]
        public void ExecuteDelete_WithoutWhereClause_ThrowsSpannerException_Sync()
        {
            var expectedSql = $"DELETE FROM `Singers` AS `s`";
            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "DELETE must have a WHERE clause [at 1:1]\nDELETE FROM `Singers` AS `s`\n^"))));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var exception = Assert.Throws<SpannerException>(() => database.Singers.ExecuteDelete());
            Assert.Equal(ErrorCode.InvalidArgument, exception.ErrorCode);
            Assert.Contains("DELETE must have a WHERE clause", exception.InnerException?.Message);

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
            Assert.Equal(2, requests.Count);
            Assert.All(requests, request => Assert.Equal(expectedSql, request.Sql));
        }

        [Fact]
        public async Task ExecuteDelete_WithoutWhereClause_ThrowsSpannerException_Async()
        {
            var expectedSql = $"DELETE FROM `Singers` AS `s`";
            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "DELETE must have a WHERE clause [at 1:1]\nDELETE FROM `Singers` AS `s`\n^"))));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var exception = await Assert.ThrowsAsync<SpannerException>(() => database.Singers.ExecuteDeleteAsync(TestContext.Current.CancellationToken));
            Assert.Equal(ErrorCode.InvalidArgument, exception.ErrorCode);
            Assert.Contains("DELETE must have a WHERE clause", exception.InnerException?.Message);

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
            Assert.Equal(2, requests.Count);
            Assert.All(requests, request => Assert.Equal(expectedSql, request.Sql));
        }

        [Fact]
        public void ExecuteDelete_Conditional_EmitsWhereConditionWithoutWhereTrue_Sync()
        {
            var expectedSql = $"DELETE FROM `Singers` AS `s`{Environment.NewLine}" +
                              $"WHERE `s`.`SingerId` > 10";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateUpdateCount(2L));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var rowsAffected = database.Singers.Where(singer => singer.SingerId > 10).ExecuteDelete();
            Assert.Equal(2L, rowsAffected);

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
        }

        [Fact]
        public async Task ExecuteDelete_Conditional_EmitsWhereConditionWithoutWhereTrue_Async()
        {
            var expectedSql = $"DELETE FROM `Singers` AS `s`{Environment.NewLine}" +
                              $"WHERE `s`.`SingerId` > 10";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateUpdateCount(2L));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var rowsAffected = await database.Singers.Where(singer => singer.SingerId > 10).ExecuteDeleteAsync(TestContext.Current.CancellationToken);
            Assert.Equal(2L, rowsAffected);

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
        }

        #endregion

        #region Bulk Update Tests

        [Fact]
        public void ExecuteUpdate_WithoutWhereClause_ThrowsSpannerException_Sync()
        {
            var expectedSql = $"UPDATE `Singers` AS `s`{Environment.NewLine}" +
                              $"SET `FirstName` = @p";
            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "UPDATE must have a WHERE clause [at 1:1]\nUPDATE `Singers` AS `s`\n^"))));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var exception = Assert.Throws<SpannerException>(() =>
                database.Singers.ExecuteUpdate(setter => setter.SetProperty(singer => singer.FirstName, "UpdatedName")));
            Assert.Equal(ErrorCode.InvalidArgument, exception.ErrorCode);
            Assert.Contains("UPDATE must have a WHERE clause", exception.InnerException?.Message);

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
            Assert.Equal(2, requests.Count);
            Assert.All(requests, request => Assert.Equal(expectedSql, request.Sql));
        }

        [Fact]
        public async Task ExecuteUpdate_WithoutWhereClause_ThrowsSpannerException_Async()
        {
            var expectedSql = $"UPDATE `Singers` AS `s`{Environment.NewLine}" +
                              $"SET `FirstName` = @p";
            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "UPDATE must have a WHERE clause [at 1:1]\nUPDATE `Singers` AS `s`\n^"))));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var exception = await Assert.ThrowsAsync<SpannerException>(() =>
                database.Singers.ExecuteUpdateAsync(setter => setter.SetProperty(singer => singer.FirstName, "UpdatedName"), TestContext.Current.CancellationToken));
            Assert.Equal(ErrorCode.InvalidArgument, exception.ErrorCode);
            Assert.Contains("UPDATE must have a WHERE clause", exception.InnerException?.Message);

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
            Assert.Equal(2, requests.Count);
            Assert.All(requests, request => Assert.Equal(expectedSql, request.Sql));
        }

        [Fact]
        public void ExecuteUpdate_Conditional_EmitsWhereConditionWithoutWhereTrue_Sync()
        {
            var expectedSql = $"UPDATE `Singers` AS `s`{Environment.NewLine}" +
                              $"SET `FirstName` = @p{Environment.NewLine}" +
                              $"WHERE `s`.`SingerId` > 10";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateUpdateCount(1L));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var rowsAffected = database.Singers.Where(singer => singer.SingerId > 10)
                .ExecuteUpdate(setter => setter.SetProperty(singer => singer.FirstName, "UpdatedName"));
            Assert.Equal(1L, rowsAffected);

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
            Assert.Single(request.Params.Fields);
            Assert.Equal("UpdatedName", request.Params.Fields["p"].StringValue);
        }

        [Fact]
        public async Task ExecuteUpdate_Conditional_EmitsWhereConditionWithoutWhereTrue_Async()
        {
            var expectedSql = $"UPDATE `Singers` AS `s`{Environment.NewLine}" +
                              $"SET `FirstName` = @p{Environment.NewLine}" +
                              $"WHERE `s`.`SingerId` > 10";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateUpdateCount(1L));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var rowsAffected = await database.Singers.Where(singer => singer.SingerId > 10)
                .ExecuteUpdateAsync(setter => setter.SetProperty(singer => singer.FirstName, "UpdatedName"), TestContext.Current.CancellationToken);
            Assert.Equal(1L, rowsAffected);

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
            Assert.Single(request.Params.Fields);
            Assert.Equal("UpdatedName", request.Params.Fields["p"].StringValue);
        }

        [Fact]
        public void ExecuteUpdate_WithInnerJoin_DoesNotEmitWhereTrue()
        {
            var expectedSql = $"UPDATE `Albums` AS `a`{Environment.NewLine}" +
                              $"SET `Title` = @p{Environment.NewLine}" +
                              $"FROM `Singers` AS `s`{Environment.NewLine}" +
                              $"WHERE `a`.`SingerId` = `s`.`SingerId`";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateUpdateCount(2L));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var rowsAffected = database.Albums
                .Join(database.Singers, album => album.SingerId, singer => singer.SingerId, (album, singer) => album)
                .ExecuteUpdate(setter => setter.SetProperty(album => album.Title, "NewTitle"));
            Assert.Equal(2L, rowsAffected);

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
            Assert.DoesNotContain("WHERE TRUE", request.Sql);
        }

        [Fact]
        public void ExecuteUpdate_WithLeftJoin_DoesNotOmitWhere()
        {
            var expectedSql = $"UPDATE `Albums` AS `a0`{Environment.NewLine}" +
                              $"SET `Title` = @p{Environment.NewLine}" +
                              $"FROM ({Environment.NewLine}" +
                              $"    SELECT `a`.`AlbumId`{Environment.NewLine}" +
                              $"    FROM `Albums` AS `a`{Environment.NewLine}" +
                              $"    LEFT JOIN `Singers` AS `s` ON `a`.`SingerId` = `s`.`SingerId`{Environment.NewLine}" +
                              $") AS `s0`{Environment.NewLine}" +
                              $"WHERE `a0`.`AlbumId` = `s0`.`AlbumId`";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateUpdateCount(1L));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var rowsAffected = database.Albums
                .GroupJoin(database.Singers, album => album.SingerId, singer => singer.SingerId, (album, singers) => new { album, singers })
                .SelectMany(group => group.singers.DefaultIfEmpty(), (group, singer) => group.album)
                .ExecuteUpdate(setter => setter.SetProperty(album => album.Title, "NewTitle"));
            Assert.Equal(1L, rowsAffected);

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
        }

        #endregion

        #region Unhappy Path Tests

        [Fact]
        public void GenerateSetOperation_IntersectAll_ThrowsNotSupportedException()
        {
            using var database = new MockServerSampleDbContext(ConnectionString);
            var serviceProvider = ((IInfrastructure<IServiceProvider>)database).Instance;
            var dependencies = serviceProvider.GetRequiredService<QuerySqlGeneratorDependencies>();
            var generator = new TestSpannerQuerySqlGenerator(dependencies);

            var aliasManager = new SqlAliasManager();
            var source1 = new SelectExpression(new SqlConstantExpression(1, null), aliasManager);
            var source2 = new SelectExpression(new SqlConstantExpression(2, null), aliasManager);
            var intersectAll = new IntersectExpression("s", source1, source2, distinct: false);

            var exception = Assert.Throws<NotSupportedException>(() => generator.GenerateSetOperationDirect(intersectAll));
            Assert.Equal("Cloud Spanner does not support 'INTERSECT ALL'.", exception.Message);
        }

        [Fact]
        public void GenerateSetOperation_ExceptAll_ThrowsNotSupportedException()
        {
            using var database = new MockServerSampleDbContext(ConnectionString);
            var serviceProvider = ((IInfrastructure<IServiceProvider>)database).Instance;
            var dependencies = serviceProvider.GetRequiredService<QuerySqlGeneratorDependencies>();
            var generator = new TestSpannerQuerySqlGenerator(dependencies);

            var aliasManager = new SqlAliasManager();
            var source1 = new SelectExpression(new SqlConstantExpression(1, null), aliasManager);
            var source2 = new SelectExpression(new SqlConstantExpression(2, null), aliasManager);
            var exceptAll = new ExceptExpression("s", source1, source2, distinct: false);

            var exception = Assert.Throws<NotSupportedException>(() => generator.GenerateSetOperationDirect(exceptAll));
            Assert.Equal("Cloud Spanner does not support 'EXCEPT ALL'.", exception.Message);
        }

        [Fact]
        public void GenerateSetOperation_UnknownSetOperation_ThrowsInvalidOperationException()
        {
            using var database = new MockServerSampleDbContext(ConnectionString);
            var serviceProvider = ((IInfrastructure<IServiceProvider>)database).Instance;
            var dependencies = serviceProvider.GetRequiredService<QuerySqlGeneratorDependencies>();
            var generator = new TestSpannerQuerySqlGenerator(dependencies);

            var aliasManager = new SqlAliasManager();
            var source1 = new SelectExpression(new SqlConstantExpression(1, null), aliasManager);
            var source2 = new SelectExpression(new SqlConstantExpression(2, null), aliasManager);
            var unknownOperation = new UnknownSetOperation("s", source1, source2, distinct: true);

            var exception = Assert.Throws<InvalidOperationException>(() => generator.GenerateSetOperationDirect(unknownOperation));
            Assert.Equal("Unknown set operation type: 'UnknownSetOperation'.", exception.Message);
        }

        [Fact]
        public void ExecuteUpdate_UpdatingPrimaryKey_ThrowsSpannerException()
        {
            var expectedSql = $"UPDATE `Singers` AS `s`{Environment.NewLine}" +
                              $"SET `SingerId` = @p{Environment.NewLine}" +
                              $"WHERE `s`.`SingerId` = 1";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateException(
                new RpcException(new Status(StatusCode.InvalidArgument, "Column 'SingerId' is part of the primary key and cannot be modified."))));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var exception = Assert.Throws<SpannerException>(() =>
                database.Singers.Where(singer => singer.SingerId == 1L).ExecuteUpdate(setter => setter.SetProperty(singer => singer.SingerId, 100L)));
            Assert.Equal(ErrorCode.InvalidArgument, exception.ErrorCode);

            var requests = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().ToList();
            Assert.Equal(2, requests.Count);
            Assert.All(requests, request => Assert.Equal(expectedSql, request.Sql));
        }

        #endregion

        #region GetOperator and Binary Expression Tests

        [Fact]
        public async Task BinaryExpression_StringConcat_EmitsConcatOperator()
        {
            using var database = new MockServerSampleDbContext(ConnectionString);
            var query = database.Singers.Select(singer => singer.FirstName + singer.LastName);
            var expectedSql = query.ToQueryString();

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateResultSet(
                new List<Tuple<SpannerTypeCode, string>> { Tuple.Create(SpannerTypeCode.String, "Concat") },
                new List<object[]> { new object[] { "AliceSinger" } }));

            var results = await query.ToListAsync(TestContext.Current.CancellationToken);
            var singleResult = Assert.Single(results);
            Assert.Equal("AliceSinger", singleResult);

            Assert.Contains("||", expectedSql);
            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
        }

        [Fact]
        public async Task BinaryExpression_NumericAddition_EmitsPlusOperator()
        {
            var expectedSql = $"SELECT `s`.`SingerId` + 5{Environment.NewLine}" +
                              $"FROM `Singers` AS `s`";

            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateResultSet(
                new List<Tuple<SpannerTypeCode, string>> { Tuple.Create(SpannerTypeCode.Int64, "Sum") },
                new List<object[]> { new object[] { "15" } }));

            using var database = new MockServerSampleDbContext(ConnectionString);
            var results = await database.Singers
                .Select(singer => singer.SingerId + 5)
                .ToListAsync(TestContext.Current.CancellationToken);
            var singleResult = Assert.Single(results);
            Assert.Equal(15L, singleResult);

            var request = Assert.Single(_fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>());
            Assert.Equal(expectedSql, request.Sql);
        }

        [Fact]
        public void GetOperator_WithNullTypeMapping_DoesNotThrowNullReferenceException()
        {
            using var database = new MockServerSampleDbContext(ConnectionString);
            var serviceProvider = ((IInfrastructure<IServiceProvider>)database).Instance;
            var dependencies = serviceProvider.GetRequiredService<QuerySqlGeneratorDependencies>();
            var querySqlGenerator = new TestSpannerQuerySqlGenerator(dependencies);

            var leftExpressionWithNullMapping = new SqlConstantExpression("left", null);
            var rightExpressionWithNullMapping = new SqlConstantExpression("right", null);
            var binaryExpressionBothNull = new SqlBinaryExpression(ExpressionType.Add, leftExpressionWithNullMapping, rightExpressionWithNullMapping, typeof(string), null);

            var operatorString = querySqlGenerator.GenerateBinaryOperator(binaryExpressionBothNull);
            Assert.Equal(" + ", operatorString);

            var typeMappingSource = serviceProvider.GetRequiredService<IRelationalTypeMappingSource>();
            var stringMapping = typeMappingSource.FindMapping(typeof(string));
            Assert.NotNull(stringMapping);

            var leftWithMapping = new SqlConstantExpression("left", stringMapping);
            var binaryExpressionRightNull = new SqlBinaryExpression(ExpressionType.Add, leftWithMapping, rightExpressionWithNullMapping, typeof(string), null);
            operatorString = querySqlGenerator.GenerateBinaryOperator(binaryExpressionRightNull);
            Assert.Equal(" + ", operatorString);

            var binaryExpressionLeftNull = new SqlBinaryExpression(ExpressionType.Add, leftExpressionWithNullMapping, leftWithMapping, typeof(string), null);
            operatorString = querySqlGenerator.GenerateBinaryOperator(binaryExpressionLeftNull);
            Assert.Equal(" + ", operatorString);

            var binaryExpressionBothMapped = new SqlBinaryExpression(ExpressionType.Add, leftWithMapping, leftWithMapping, typeof(string), stringMapping);
            operatorString = querySqlGenerator.GenerateBinaryOperator(binaryExpressionBothMapped);
            Assert.Equal("||", operatorString);
        }

        #endregion
    }
}
