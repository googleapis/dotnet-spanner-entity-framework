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

#pragma warning disable EF1001

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.V1;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests;

public class SpannerInExpressionTests : IClassFixture<SpannerMockServerFixture>
{
    private readonly SpannerMockServerFixture _fixture;

    public SpannerInExpressionTests(SpannerMockServerFixture service)
    {
        _fixture = service;
        service.SpannerMock.Reset();
    }

    private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

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
                new object[] { "1", "1990-01-01", "Alice", "Alice Richards", "Richards", null },
            }
        ));
    }

    [Fact]
    public async Task FindSingers_ContainsOnNullableColumn_EmitsInUnnest()
    {
        var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                  $"FROM `Singers` AS `s`{Environment.NewLine}" +
                  $"WHERE `s`.`FirstName` IN  UNNEST (@names)";
        AddFindSingerResult(sql);

        var names = new List<string> { "Alice", "Bob" };
        await using var db = new MockServerSampleDbContext(ConnectionString);
        var cancellationToken = TestContext.Current.CancellationToken;
        var singers = await db.Singers
            .Where(singer => names.Contains(singer.FirstName))
            .ToListAsync(cancellationToken);

        Assert.Single(singers);
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request =>
            {
                Assert.Equal(sql, request.Sql);
                Assert.Single(request.Params.Fields);
                var fields = request.Params.Fields;
                Assert.Equal(2, fields["names"].ListValue.Values.Count);
                Assert.Equal("Alice", fields["names"].ListValue.Values[0].StringValue);
                Assert.Equal("Bob", fields["names"].ListValue.Values[1].StringValue);
                Assert.Single(request.ParamTypes);
                var type = request.ParamTypes["names"];
                Assert.Equal(V1.TypeCode.Array, type.Code);
                Assert.Equal(V1.TypeCode.String, type.ArrayElementType.Code);
            }
        );
    }

    [Fact]
    public async Task FindSingers_NotContainsOnNullableColumn_EmitsNullSafeNotInUnnest()
    {
        var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                  $"FROM `Singers` AS `s`{Environment.NewLine}" +
                  $"WHERE `s`.`FirstName` NOT IN  UNNEST (@names) OR `s`.`FirstName` IS NULL";
        AddFindSingerResult(sql);

        var names = new List<string> { "Alice", "Bob" };
        await using var db = new MockServerSampleDbContext(ConnectionString);
        var cancellationToken = TestContext.Current.CancellationToken;
        var singers = await db.Singers
            .Where(singer => !names.Contains(singer.FirstName))
            .ToListAsync(cancellationToken);

        Assert.Single(singers);
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request =>
            {
                Assert.Equal(sql, request.Sql);
                Assert.Single(request.Params.Fields);
                var fields = request.Params.Fields;
                Assert.Equal(2, fields["names"].ListValue.Values.Count);
                Assert.Equal("Alice", fields["names"].ListValue.Values[0].StringValue);
                Assert.Equal("Bob", fields["names"].ListValue.Values[1].StringValue);
                Assert.Single(request.ParamTypes);
                var type = request.ParamTypes["names"];
                Assert.Equal(V1.TypeCode.Array, type.Code);
                Assert.Equal(V1.TypeCode.String, type.ArrayElementType.Code);
            }
        );
    }

    [Fact]
    public async Task SelectSingers_ContainsOnNullableColumn_EmitsNullSafeInUnnestInProjection()
    {
        var sql = $"SELECT `s`.`FirstName` IN  UNNEST (@names) AND `s`.`FirstName` IS NOT NULL{Environment.NewLine}" +
                  $"FROM `Singers` AS `s`";
        _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<V1.TypeCode, string>>
            {
                Tuple.Create(V1.TypeCode.Bool, "Result"),
            },
            new List<object[]>
            {
                new object[] { true },
            }
        ));

        var names = new List<string> { "Alice", "Bob" };
        await using var db = new MockServerSampleDbContext(ConnectionString);
        var cancellationToken = TestContext.Current.CancellationToken;
        var results = await db.Singers
            .Select(singer => names.Contains(singer.FirstName))
            .ToListAsync(cancellationToken);

        Assert.Single(results);
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request =>
            {
                Assert.Equal(sql, request.Sql);
                Assert.Single(request.Params.Fields);
                var fields = request.Params.Fields;
                Assert.Equal(2, fields["names"].ListValue.Values.Count);
                Assert.Equal("Alice", fields["names"].ListValue.Values[0].StringValue);
                Assert.Equal("Bob", fields["names"].ListValue.Values[1].StringValue);
                Assert.Single(request.ParamTypes);
                var type = request.ParamTypes["names"];
                Assert.Equal(V1.TypeCode.Array, type.Code);
                Assert.Equal(V1.TypeCode.String, type.ArrayElementType.Code);
            }
        );
    }

    [Fact]
    public async Task SelectSingers_NotContainsOnNullableColumn_EmitsNullSafeNotInUnnestInProjection()
    {
        var sql = $"SELECT `s`.`FirstName` NOT IN  UNNEST (@names) OR `s`.`FirstName` IS NULL{Environment.NewLine}" +
                  $"FROM `Singers` AS `s`";
        _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<V1.TypeCode, string>>
            {
                Tuple.Create(V1.TypeCode.Bool, "Result"),
            },
            new List<object[]>
            {
                new object[] { true },
            }
        ));

        var names = new List<string> { "Alice", "Bob" };
        await using var db = new MockServerSampleDbContext(ConnectionString);
        var cancellationToken = TestContext.Current.CancellationToken;
        var results = await db.Singers
            .Select(singer => !names.Contains(singer.FirstName))
            .ToListAsync(cancellationToken);

        Assert.Single(results);
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request =>
            {
                Assert.Equal(sql, request.Sql);
                Assert.Single(request.Params.Fields);
                var fields = request.Params.Fields;
                Assert.Equal(2, fields["names"].ListValue.Values.Count);
                Assert.Equal("Alice", fields["names"].ListValue.Values[0].StringValue);
                Assert.Equal("Bob", fields["names"].ListValue.Values[1].StringValue);
                Assert.Single(request.ParamTypes);
                var type = request.ParamTypes["names"];
                Assert.Equal(V1.TypeCode.Array, type.Code);
                Assert.Equal(V1.TypeCode.String, type.ArrayElementType.Code);
            }
        );
    }

    [Fact]
    public async Task SelectSingers_ContainsOnNonNullableColumn_EmitsDirectInUnnestInProjection()
    {
        var sql = $"SELECT `s`.`LastName` IN  UNNEST (@names){Environment.NewLine}" +
                  $"FROM `Singers` AS `s`";
        _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<V1.TypeCode, string>>
            {
                Tuple.Create(V1.TypeCode.Bool, "Result"),
            },
            new List<object[]>
            {
                new object[] { true },
            }
        ));

        var names = new List<string> { "Richards", "Smith" };
        await using var db = new MockServerSampleDbContext(ConnectionString);
        var cancellationToken = TestContext.Current.CancellationToken;
        var results = await db.Singers
            .Select(singer => names.Contains(singer.LastName))
            .ToListAsync(cancellationToken);

        Assert.Single(results);
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request =>
            {
                Assert.Equal(sql, request.Sql);
                Assert.Single(request.Params.Fields);
                var fields = request.Params.Fields;
                Assert.Equal(2, fields["names"].ListValue.Values.Count);
                Assert.Equal("Richards", fields["names"].ListValue.Values[0].StringValue);
                Assert.Equal("Smith", fields["names"].ListValue.Values[1].StringValue);
                Assert.Single(request.ParamTypes);
                var type = request.ParamTypes["names"];
                Assert.Equal(V1.TypeCode.Array, type.Code);
                Assert.Equal(V1.TypeCode.String, type.ArrayElementType.Code);
            }
        );
    }

    [Fact]
    public async Task SelectSingers_WithRelationalNulls_EmitsDirectInUnnestInProjection()
    {
        var sql = $"SELECT `s`.`FirstName` IN  UNNEST (@names){Environment.NewLine}" +
                  $"FROM `Singers` AS `s`";
        _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
            new List<Tuple<V1.TypeCode, string>>
            {
                Tuple.Create(V1.TypeCode.Bool, "Result"),
            },
            new List<object[]>
            {
                new object[] { true },
            }
        ));

        var optionsBuilder = new DbContextOptionsBuilder<SpannerSampleDbContext>();
        optionsBuilder
            .UseSpanner(ConnectionString, spannerOptions =>
            {
                spannerOptions.UseRelationalNulls();
                SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false);
            }, ChannelCredentials.Insecure)
            .UseMutations(MutationUsage.Never)
            .UseLazyLoadingProxies();

        var names = new List<string> { "Alice", "Bob" };
        await using var db = new SpannerSampleDbContext(optionsBuilder.Options);
        var cancellationToken = TestContext.Current.CancellationToken;
        var results = await db.Singers
            .Select(singer => names.Contains(singer.FirstName))
            .ToListAsync(cancellationToken);

        Assert.Single(results);
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request =>
            {
                Assert.Equal(sql, request.Sql);
                Assert.Single(request.Params.Fields);
                var fields = request.Params.Fields;
                Assert.Equal(2, fields["names"].ListValue.Values.Count);
                Assert.Equal("Alice", fields["names"].ListValue.Values[0].StringValue);
                Assert.Equal("Bob", fields["names"].ListValue.Values[1].StringValue);
                Assert.Single(request.ParamTypes);
                var type = request.ParamTypes["names"];
                Assert.Equal(V1.TypeCode.Array, type.Code);
                Assert.Equal(V1.TypeCode.String, type.ArrayElementType.Code);
            }
        );
    }

    [Fact]
    public async Task FindSingers_ContainsWithEmptyList_EmitsInUnnest()
    {
        var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                  $"FROM `Singers` AS `s`{Environment.NewLine}" +
                  $"WHERE `s`.`FirstName` IN  UNNEST (@names)";
        AddFindSingerResult(sql);

        var names = new List<string>();
        await using var db = new MockServerSampleDbContext(ConnectionString);
        var cancellationToken = TestContext.Current.CancellationToken;
        var singers = await db.Singers
            .Where(singer => names.Contains(singer.FirstName))
            .ToListAsync(cancellationToken);

        Assert.Single(singers);
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request =>
            {
                Assert.Equal(sql, request.Sql);
                Assert.Single(request.Params.Fields);
                var fields = request.Params.Fields;
                Assert.Empty(fields["names"].ListValue.Values);
                Assert.Single(request.ParamTypes);
                var type = request.ParamTypes["names"];
                Assert.Equal(V1.TypeCode.Array, type.Code);
                Assert.Equal(V1.TypeCode.String, type.ArrayElementType.Code);
            }
        );
    }

    [Fact]
    public async Task FindSingers_ContainsWithListContainingNull_EmitsInUnnest()
    {
        var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                  $"FROM `Singers` AS `s`{Environment.NewLine}" +
                  $"WHERE `s`.`FirstName` IN  UNNEST (@names)";
        AddFindSingerResult(sql);

        var names = new List<string> { "Alice", null };
        await using var db = new MockServerSampleDbContext(ConnectionString);
        var cancellationToken = TestContext.Current.CancellationToken;
        var singers = await db.Singers
            .Where(singer => names.Contains(singer.FirstName))
            .ToListAsync(cancellationToken);

        Assert.Single(singers);
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request =>
            {
                Assert.Equal(sql, request.Sql);
                Assert.Single(request.Params.Fields);
                var fields = request.Params.Fields;
                Assert.Equal(2, fields["names"].ListValue.Values.Count);
                Assert.Equal("Alice", fields["names"].ListValue.Values[0].StringValue);
                Assert.Equal(Value.KindOneofCase.NullValue, fields["names"].ListValue.Values[1].KindCase);
                Assert.Single(request.ParamTypes);
                var type = request.ParamTypes["names"];
                Assert.Equal(V1.TypeCode.Array, type.Code);
                Assert.Equal(V1.TypeCode.String, type.ArrayElementType.Code);
            }
        );
    }

    [Fact]
    public async Task FindSingers_NotContainsOnNonNullableColumn_EmitsDirectNotInUnnest()
    {
        var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                  $"FROM `Singers` AS `s`{Environment.NewLine}" +
                  $"WHERE `s`.`LastName` NOT IN  UNNEST (@names)";
        AddFindSingerResult(sql);

        var names = new List<string> { "Richards", "Smith" };
        await using var db = new MockServerSampleDbContext(ConnectionString);
        var cancellationToken = TestContext.Current.CancellationToken;
        var singers = await db.Singers
            .Where(singer => !names.Contains(singer.LastName))
            .ToListAsync(cancellationToken);

        Assert.Single(singers);
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request =>
            {
                Assert.Equal(sql, request.Sql);
                Assert.Single(request.Params.Fields);
                var fields = request.Params.Fields;
                Assert.Equal(2, fields["names"].ListValue.Values.Count);
                Assert.Equal("Richards", fields["names"].ListValue.Values[0].StringValue);
                Assert.Equal("Smith", fields["names"].ListValue.Values[1].StringValue);
                Assert.Single(request.ParamTypes);
                var type = request.ParamTypes["names"];
                Assert.Equal(V1.TypeCode.Array, type.Code);
                Assert.Equal(V1.TypeCode.String, type.ArrayElementType.Code);
            }
        );
    }

    [Fact]
    public async Task FindSingers_WithRelationalNulls_EmitsDirectNotInUnnest()
    {
        var sql = $"SELECT `s`.`SingerId`, `s`.`BirthDate`, `s`.`FirstName`, `s`.`FullName`, `s`.`LastName`, `s`.`Picture`{Environment.NewLine}" +
                  $"FROM `Singers` AS `s`{Environment.NewLine}" +
                  $"WHERE `s`.`FirstName` NOT IN  UNNEST (@names)";
        AddFindSingerResult(sql);

        var optionsBuilder = new DbContextOptionsBuilder<SpannerSampleDbContext>();
        optionsBuilder
            .UseSpanner(ConnectionString, spannerOptions =>
            {
                spannerOptions.UseRelationalNulls();
                SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false);
            }, ChannelCredentials.Insecure)
            .UseMutations(MutationUsage.Never)
            .UseLazyLoadingProxies();

        var names = new List<string> { "Alice", "Bob" };
        await using var db = new SpannerSampleDbContext(optionsBuilder.Options);
        var cancellationToken = TestContext.Current.CancellationToken;
        var singers = await db.Singers
            .Where(singer => !names.Contains(singer.FirstName))
            .ToListAsync(cancellationToken);

        Assert.Single(singers);
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request =>
            {
                Assert.Equal(sql, request.Sql);
                Assert.Single(request.Params.Fields);
                var fields = request.Params.Fields;
                Assert.Equal(2, fields["names"].ListValue.Values.Count);
                Assert.Equal("Alice", fields["names"].ListValue.Values[0].StringValue);
                Assert.Equal("Bob", fields["names"].ListValue.Values[1].StringValue);
                Assert.Single(request.ParamTypes);
                var type = request.ParamTypes["names"];
                Assert.Equal(V1.TypeCode.Array, type.Code);
                Assert.Equal(V1.TypeCode.String, type.ArrayElementType.Code);
            }
        );
    }

    [Fact]
    public void SpannerInExpression_Overrides_BehaveCorrectly()
    {
        var boolMapping = SpannerBoolTypeMapping.Default;
        var item1 = new ColumnExpression("Col1", "t", typeof(string), SpannerStringTypeMapping.Default, true);
        var item2 = new ColumnExpression("Col2", "t", typeof(string), SpannerStringTypeMapping.Default, true);
        var valuesParameter1 = new SqlParameterExpression("p1", typeof(List<string>), SpannerComplexTypeMapping.Default);
        var valuesParameter2 = new SqlParameterExpression("p2", typeof(List<string>), SpannerComplexTypeMapping.Default);

        var spannerIn = new SpannerInExpression(item1, valuesParameter1, boolMapping);

        // Same references return this
        Assert.Same(spannerIn, spannerIn.Update(item1, valuesParameter1));
        Assert.Same(spannerIn, spannerIn.Update(item1, null, null, valuesParameter1));

        // Different item returns new SpannerInExpression
        var updatedItem = spannerIn.Update(item2, valuesParameter1);
        var inItem = Assert.IsType<SpannerInExpression>(updatedItem);
        Assert.Equal(item2, inItem.Item);
        Assert.Equal(valuesParameter1, inItem.ValuesParameter);

        // Providing values or subquery falls back to base InExpression
        var fallbackUpdated = spannerIn.Update(item1, null, new[] { item2 }, null);
        Assert.IsType<InExpression>(fallbackUpdated);
        Assert.IsNotType<SpannerInExpression>(fallbackUpdated);

        // Different values parameter returns new SpannerInExpression
        var updatedParameter = spannerIn.Update(item1, valuesParameter2);
        var inParameter = Assert.IsType<SpannerInExpression>(updatedParameter);
        Assert.Equal(item1, inParameter.Item);
        Assert.Equal(valuesParameter2, inParameter.ValuesParameter);

        // ApplyTypeMapping preserves SpannerInExpression
        var remapped = spannerIn.ApplyTypeMapping(boolMapping);
        Assert.IsType<SpannerInExpression>(remapped);

        // Equals and HashCode
        var identical = new SpannerInExpression(item1, valuesParameter1, boolMapping);
        Assert.Equal(spannerIn, identical);
        Assert.Equal(spannerIn.GetHashCode(), identical.GetHashCode());
        Assert.NotEqual(spannerIn, updatedItem);

        // Quote produces NewExpression with SpannerInExpression constructor
        var quoted = spannerIn.Quote();
        var newExpression = Assert.IsType<NewExpression>(quoted);
        Assert.Equal(typeof(SpannerInExpression), newExpression.Type);
    }

    [Fact]
    public void SpannerValueExpression_Subclasses_ImplementUpdate()
    {
        using var db = new MockServerSampleDbContext(ConnectionString);
        var sqlExpressionFactory = db.GetService<ISqlExpressionFactory>();

        var column1 = new ColumnExpression("DateCol1", "t", typeof(DateTime), SpannerTimestampTypeMapping.Default, true);
        var column2 = new ColumnExpression("DateCol2", "t", typeof(DateTime), SpannerTimestampTypeMapping.Default, true);

        // SpannerDateExtractExpression
        var dateExtract = new SpannerDateExtractExpression(sqlExpressionFactory, column1, "YEAR");
        Assert.Same(dateExtract, dateExtract.Update(column1));
        var updatedDateExtract = dateExtract.Update(column2);
        var extractResult = Assert.IsType<SpannerDateExtractExpression>(updatedDateExtract);
        Assert.Equal(column2, extractResult.Value);

        // SpannerIntervalExpression
        var intervalExpression = new SpannerIntervalExpression(sqlExpressionFactory, column1, "DAY");
        Assert.Same(intervalExpression, intervalExpression.Update(column1));
        var updatedInterval = intervalExpression.Update(column2);
        var intervalResult = Assert.IsType<SpannerIntervalExpression>(updatedInterval);
        Assert.Equal(column2, intervalResult.Value);

        // SpannerTimestampExtractExpression
        var timestampExtract = new SpannerTimestampExtractExpression(sqlExpressionFactory, column1, "HOUR");
        Assert.Same(timestampExtract, timestampExtract.Update(column1));
        var updatedTimestampExtract = timestampExtract.Update(column2);
        var timestampResult = Assert.IsType<SpannerTimestampExtractExpression>(updatedTimestampExtract);
        Assert.Equal(column2, timestampResult.Value);
    }

    [Fact]
    public void SpannerSqlExpressionFactory_In_FallsBackForNonArrayParameter()
    {
        using var db = new MockServerSampleDbContext(ConnectionString);
        var sqlExpressionFactory = db.GetService<ISqlExpressionFactory>();
        var typeMappingSource = db.GetService<IRelationalTypeMappingSource>();

        var bytesMapping = typeMappingSource.FindMapping(typeof(byte[]))!;
        var column = new ColumnExpression("Picture", "Singers", typeof(byte[]), bytesMapping, true);
        var parameter = new SqlParameterExpression("picture", typeof(byte[]), bytesMapping);

        var inExpression = sqlExpressionFactory.In(column, parameter);

        // A non-array parameter (e.g. byte[] mapping to BYTES) must not produce SpannerInExpression
        Assert.IsNotType<SpannerInExpression>(inExpression);
        Assert.IsType<InExpression>(inExpression);
    }

    [Fact]
    public void SpannerSqlNullabilityProcessor_VisitIn_FallsBackForBaseInExpression()
    {
        using var db = new MockServerSampleDbContext(ConnectionString);
        var sqlExpressionFactory = db.GetService<ISqlExpressionFactory>();
        var dependencies = db.GetService<RelationalParameterBasedSqlProcessorDependencies>();
        var parameters = new RelationalParameterBasedSqlProcessorParameters(useRelationalNulls: false, default);
        var processor = new TestSpannerSqlNullabilityProcessor(dependencies, parameters);

        var item = new ColumnExpression("LastName", "Singers", typeof(string), SpannerStringTypeMapping.Default, false);
        var constant1 = sqlExpressionFactory.Constant("Smith", SpannerStringTypeMapping.Default);
        var constant2 = sqlExpressionFactory.Constant("Jones", SpannerStringTypeMapping.Default);
        var inExpression = Assert.IsType<InExpression>(sqlExpressionFactory.In(item, new[] { constant1, constant2 }));

        Assert.IsNotType<SpannerInExpression>(inExpression);
        var processed = processor.TestVisitIn(inExpression, allowOptimizedExpansion: true, out var nullable);
        Assert.NotNull(processed);
        Assert.IsNotType<SpannerInExpression>(processed);
        Assert.False(nullable);
    }

    [Fact]
    public void SpannerSqlNullabilityProcessor_VisitsSpannerValueExpression_PropagatingNullability()
    {
        using var db = new MockServerSampleDbContext(ConnectionString);
        var sqlExpressionFactory = db.GetService<ISqlExpressionFactory>();
        var dependencies = db.GetService<RelationalParameterBasedSqlProcessorDependencies>();
        var parameters = new RelationalParameterBasedSqlProcessorParameters(useRelationalNulls: false, default);
        var processor = new TestSpannerSqlNullabilityProcessor(dependencies, parameters);

        var nullableColumn = new ColumnExpression("BirthDate", "Singers", typeof(DateTime), SpannerTimestampTypeMapping.Default, true);
        var nonNullableColumn = new ColumnExpression("BirthDate", "Singers", typeof(DateTime), SpannerTimestampTypeMapping.Default, false);

        var nullableDateExtract = new SpannerDateExtractExpression(sqlExpressionFactory, nullableColumn, "YEAR");
        var nonNullableDateExtract = new SpannerDateExtractExpression(sqlExpressionFactory, nonNullableColumn, "YEAR");

        var processedNullable = processor.TestVisitCustomSqlExpression(nullableDateExtract, allowOptimizedExpansion: false, out var isNullable);
        Assert.IsType<SpannerDateExtractExpression>(processedNullable);
        Assert.True(isNullable);

        var processedNonNullable = processor.TestVisitCustomSqlExpression(nonNullableDateExtract, allowOptimizedExpansion: false, out isNullable);
        Assert.IsType<SpannerDateExtractExpression>(processedNonNullable);
        Assert.False(isNullable);
    }

    [Fact]
    public void SpannerSqlNullabilityProcessor_VisitsSpannerContains_CompensatesNullability()
    {
        using var db = new MockServerSampleDbContext(ConnectionString);
        var sqlExpressionFactory = db.GetService<ISqlExpressionFactory>();
        var dependencies = db.GetService<RelationalParameterBasedSqlProcessorDependencies>();
        var parameters = new RelationalParameterBasedSqlProcessorParameters(useRelationalNulls: false, default);
        var processor = new TestSpannerSqlNullabilityProcessor(dependencies, parameters);

        var nullableColumn = new ColumnExpression("FirstName", "Singers", typeof(string), SpannerStringTypeMapping.Default, true);
        var nonNullableColumn = new ColumnExpression("LastName", "Singers", typeof(string), SpannerStringTypeMapping.Default, false);
        var arrayColumn = new ColumnExpression("Nicknames", "Singers", typeof(List<string>), SpannerComplexTypeMapping.Default, false);
        var values = sqlExpressionFactory.Function("UNNEST", new[] { arrayColumn }, false, new[] { true }, typeof(List<string>));

        var nullableContains = new SpannerContainsExpression(nullableColumn, values, negated: false, SpannerBoolTypeMapping.Default);
        var nonNullableContains = new SpannerContainsExpression(nonNullableColumn, values, negated: false, SpannerBoolTypeMapping.Default);

        // When allowOptimizedExpansion is true (WHERE clause): nullable is true, returns SpannerContainsExpression directly
        var optimized = processor.TestVisitCustomSqlExpression(nullableContains, allowOptimizedExpansion: true, out var isNullable);
        Assert.IsType<SpannerContainsExpression>(optimized);
        Assert.True(isNullable);

        // When allowOptimizedExpansion is false (projection/NOT): wraps with AndAlso(..., IsNotNull)
        var nonOptimized = processor.TestVisitCustomSqlExpression(nullableContains, allowOptimizedExpansion: false, out isNullable);
        var andAlsoExpression = Assert.IsType<SqlBinaryExpression>(nonOptimized);
        Assert.Equal(ExpressionType.AndAlso, andAlsoExpression.OperatorType);
        Assert.False(isNullable);

        // When item is non-nullable: returns SpannerContainsExpression directly without null compensation
        var nonNullableResult = processor.TestVisitCustomSqlExpression(nonNullableContains, allowOptimizedExpansion: false, out isNullable);
        Assert.IsType<SpannerContainsExpression>(nonNullableResult);
        Assert.False(isNullable);

        // When relational nulls are enabled: returns SpannerContainsExpression directly
        var relationalNullsParameters = new RelationalParameterBasedSqlProcessorParameters(useRelationalNulls: true, default);
        var relationalNullsProcessor = new TestSpannerSqlNullabilityProcessor(dependencies, relationalNullsParameters);
        var relationalResult = relationalNullsProcessor.TestVisitCustomSqlExpression(nullableContains, allowOptimizedExpansion: false, out isNullable);
        Assert.IsType<SpannerContainsExpression>(relationalResult);
        Assert.True(isNullable);
    }

    private sealed class TestSpannerSqlNullabilityProcessor : SpannerSqlNullabilityProcessor
    {
        public TestSpannerSqlNullabilityProcessor(
            RelationalParameterBasedSqlProcessorDependencies dependencies,
            RelationalParameterBasedSqlProcessorParameters parameters)
            : base(dependencies, parameters)
        {
        }

        public SqlExpression TestVisitIn(InExpression inExpression, bool allowOptimizedExpansion, out bool nullable)
            => VisitIn(inExpression, allowOptimizedExpansion, out nullable);

        public SqlExpression TestVisitCustomSqlExpression(SqlExpression sqlExpression, bool allowOptimizedExpansion, out bool nullable)
            => VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable);
    }
}
