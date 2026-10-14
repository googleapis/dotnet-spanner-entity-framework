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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;
using SpannerDate = Google.Cloud.EntityFrameworkCore.Spanner.Storage.SpannerDate;
using V1 = Google.Cloud.Spanner.V1;

#pragma warning disable EF1001
namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests;

public class DefaultTypeMappingMockServerTests : IClassFixture<SpannerMockServerFixture>
{
    private readonly SpannerMockServerFixture _fixture;

    public DefaultTypeMappingMockServerTests(SpannerMockServerFixture fixture)
    {
        _fixture = fixture;
        fixture.SpannerMock.Reset();
    }

    private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

    public class EntityWithDefaultTypeMappings
    {
        public long Id { get; set; }
        public SpannerDate Date { get; set; }
        public DateOnly DateOnly { get; set; }
        public JsonDocument Json { get; set; }
        public JsonDocument[] JsonArray { get; set; }
        public SpannerDate[] DateArray { get; set; }
        public List<SpannerDate> DateList { get; set; }
        public SpannerDate?[] NullableDateArray { get; set; }
        public List<SpannerDate?> NullableDateList { get; set; }
        public List<JsonDocument> JsonList { get; set; }
    }

    private class TestDefaultMappingDbContext : DbContext
    {
        private readonly string _connectionString;

        public DbSet<EntityWithDefaultTypeMappings> Entities { get; set; }

        public TestDefaultMappingDbContext(string connectionString)
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                    .UseMutations(MutationUsage.Never);
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<EntityWithDefaultTypeMappings>(entity =>
            {
                entity.HasKey(record => record.Id);
                entity.ToTable("EntitiesWithDefaultTypeMappings");
                entity.Property(record => record.Date).Metadata.SetTypeMapping(SpannerDateTypeMapping.Default);
                entity.Property(record => record.DateOnly).Metadata.SetTypeMapping(SpannerDateOnlyTypeMapping.Default);
                entity.Property(record => record.Json).Metadata.SetTypeMapping(SpannerJsonTypeMapping.Default);
                entity.Property(record => record.JsonArray).Metadata.SetTypeMapping(SpannerJsonArrayTypeMapping.Default);
                entity.Property(record => record.DateArray).Metadata.SetTypeMapping(SpannerDateArrayTypeMapping.Default);
                entity.Property(record => record.DateList).Metadata.SetTypeMapping(SpannerDateListTypeMapping.Default);
                entity.Property(record => record.NullableDateArray).Metadata.SetTypeMapping(SpannerNullableDateArrayTypeMapping.Default);
                entity.Property(record => record.NullableDateList).Metadata.SetTypeMapping(SpannerNullableDateListTypeMapping.Default);
                entity.Property(record => record.JsonList).Metadata.SetTypeMapping(SpannerJsonListTypeMapping.Default);
            });
        }
    }

    [Fact]
    public async Task Query_EntityWithDefaultTypeMappings_MaterializesValuesCorrectly()
    {
        await using var context = new TestDefaultMappingDbContext(ConnectionString);

        var query = context.Entities.Select(record => new
        {
            record.Id,
            record.Date,
            record.DateOnly,
            record.Json,
            record.JsonArray,
            record.DateArray,
            record.DateList,
            record.NullableDateArray,
            record.NullableDateList,
            record.JsonList,
        });

        var expectedSql = $"{query.ToQueryString()}{Environment.NewLine}LIMIT 1";

        var dateValue = new DateTime(2025, 5, 20);
        var expectedSpannerDate = new SpannerDate(2025, 5, 20);
        var expectedDateOnly = new DateOnly(2025, 5, 20);
        var jsonString = "{\"key\":\"value\"}";

        _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateResultSet(
            new List<Tuple<V1.Type, string>>
            {
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Int64 }, "Id"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Date }, "Date"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Date }, "DateOnly"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Json }, "Json"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Json } }, "JsonArray"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Date } }, "DateArray"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Date } }, "DateList"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Date } }, "NullableDateArray"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Date } }, "NullableDateList"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Json } }, "JsonList"),
            },
            new List<object[]>
            {
                new object[]
                {
                    1L,
                    dateValue,
                    dateValue,
                    jsonString,
                    new List<string> { jsonString },
                    new List<DateTime> { dateValue },
                    new List<DateTime> { dateValue },
                    new List<DateTime?> { dateValue, null },
                    new List<DateTime?> { dateValue, null },
                    new List<string> { jsonString },
                },
            }));

        var result = await query.FirstOrDefaultAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.Equal(1L, result.Id);
        Assert.Equal(expectedSpannerDate, result.Date);
        Assert.Equal(expectedDateOnly, result.DateOnly);
        Assert.Equal("value", result.Json.RootElement.GetProperty("key").GetString());
        Assert.Single(result.JsonArray);
        Assert.Equal("value", result.JsonArray[0].RootElement.GetProperty("key").GetString());
        Assert.Single(result.DateArray);
        Assert.Equal(expectedSpannerDate, result.DateArray[0]);
        Assert.Single(result.DateList);
        Assert.Equal(expectedSpannerDate, result.DateList[0]);
        Assert.Equal(2, result.NullableDateArray.Length);
        Assert.Equal(expectedSpannerDate, result.NullableDateArray[0]);
        Assert.Null(result.NullableDateArray[1]);
        Assert.Equal(2, result.NullableDateList.Count);
        Assert.Equal(expectedSpannerDate, result.NullableDateList[0]);
        Assert.Null(result.NullableDateList[1]);
        Assert.Single(result.JsonList);
        Assert.Equal("value", result.JsonList[0].RootElement.GetProperty("key").GetString());
    }

    [Fact]
    public async Task Insert_EntityWithDefaultTypeMappings_ConvertsParametersCorrectly()
    {
        var insertSql = $"INSERT INTO `EntitiesWithDefaultTypeMappings` (`Id`, `Date`, `DateArray`, `DateList`, `DateOnly`, `Json`, `JsonArray`, `JsonList`, `NullableDateArray`, `NullableDateList`){Environment.NewLine}" +
                        $"VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9)";
        _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

        await using var context = new TestDefaultMappingDbContext(ConnectionString);

        var spannerDate = new SpannerDate(2025, 5, 20);
        var dateOnly = new DateOnly(2025, 5, 20);
        using var jsonDocument = JsonDocument.Parse("{\"key\":\"value\"}");

        context.Entities.Add(new EntityWithDefaultTypeMappings
        {
            Id = 1L,
            Date = spannerDate,
            DateOnly = dateOnly,
            Json = jsonDocument,
            JsonArray = new[] { jsonDocument },
            DateArray = new[] { spannerDate },
            DateList = new List<SpannerDate> { spannerDate },
            NullableDateArray = new SpannerDate?[] { spannerDate, null },
            NullableDateList = new List<SpannerDate?> { spannerDate, null },
            JsonList = new List<JsonDocument> { jsonDocument },
        });

        var count = await context.SaveChangesAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, count);

        var batchDmlRequest = _fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>().FirstOrDefault();
        var executeSqlRequest = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().FirstOrDefault(request => request.Sql == insertSql);
        var statement = batchDmlRequest != null
            ? batchDmlRequest.Statements.FirstOrDefault(statement => statement.Sql == insertSql)
            : executeSqlRequest?.Params != null
                ? new ExecuteBatchDmlRequest.Types.Statement
                {
                    Sql = insertSql,
                    Params = executeSqlRequest.Params,
                }
                : null;

        Assert.NotNull(statement);
        var fields = statement.Params.Fields;
        Assert.Equal("1", fields["p0"].StringValue);
        Assert.Equal("2025-05-20", fields["p1"].StringValue);

        Assert.Single(fields["p2"].ListValue.Values);
        Assert.Equal("2025-05-20", fields["p2"].ListValue.Values[0].StringValue);

        Assert.Single(fields["p3"].ListValue.Values);
        Assert.Equal("2025-05-20", fields["p3"].ListValue.Values[0].StringValue);

        Assert.Equal("2025-05-20", fields["p4"].StringValue);
        Assert.Equal("{\"key\":\"value\"}", fields["p5"].StringValue);

        Assert.Single(fields["p6"].ListValue.Values);
        Assert.Equal("{\"key\":\"value\"}", fields["p6"].ListValue.Values[0].StringValue);

        Assert.Single(fields["p7"].ListValue.Values);
        Assert.Equal("{\"key\":\"value\"}", fields["p7"].ListValue.Values[0].StringValue);

        Assert.Equal(2, fields["p8"].ListValue.Values.Count);
        Assert.Equal("2025-05-20", fields["p8"].ListValue.Values[0].StringValue);
        Assert.Equal(Google.Protobuf.WellKnownTypes.NullValue.NullValue, fields["p8"].ListValue.Values[1].NullValue);

        Assert.Equal(2, fields["p9"].ListValue.Values.Count);
        Assert.Equal("2025-05-20", fields["p9"].ListValue.Values[0].StringValue);
        Assert.Equal(Google.Protobuf.WellKnownTypes.NullValue.NullValue, fields["p9"].ListValue.Values[1].NullValue);
    }

    [Fact]
    public async Task Query_EntityWithDefaultTypeMappings_WhereFilter_ConvertsParametersCorrectly()
    {
        await using var context = new TestDefaultMappingDbContext(ConnectionString);

        var filterDate = new SpannerDate(2025, 5, 20);
        var filterDateOnly = new DateOnly(2025, 5, 20);

        var expectedSql = $"SELECT `e`.`Id`, `e`.`Date`, `e`.`DateArray`, `e`.`DateList`, `e`.`DateOnly`, `e`.`Json`, `e`.`JsonArray`, `e`.`JsonList`, `e`.`NullableDateArray`, `e`.`NullableDateList`{Environment.NewLine}" +
                          $"FROM `EntitiesWithDefaultTypeMappings` AS `e`{Environment.NewLine}" +
                          $"WHERE `e`.`Date` = @filterDate AND `e`.`DateOnly` = @filterDateOnly";

        _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateResultSet(
            new List<Tuple<V1.Type, string>>
            {
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Int64 }, "Id"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Date }, "Date"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Date }, "DateOnly"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Json }, "Json"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Json } }, "JsonArray"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Date } }, "DateArray"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Date } }, "DateList"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Date } }, "NullableDateArray"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Date } }, "NullableDateList"),
                Tuple.Create(new V1.Type { Code = V1.TypeCode.Array, ArrayElementType = new V1.Type { Code = V1.TypeCode.Json } }, "JsonList"),
            },
            new List<object[]>()));

        var results = await context.Entities
            .Where(record => record.Date == filterDate && record.DateOnly == filterDateOnly)
            .ToListAsync(TestContext.Current.CancellationToken);
        Assert.Empty(results);

        var executeSqlRequest = _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>().FirstOrDefault(request => request.Sql == expectedSql);
        Assert.NotNull(executeSqlRequest);
        Assert.Equal("2025-05-20", executeSqlRequest.Params.Fields["filterDate"].StringValue);
        Assert.Equal("2025-05-20", executeSqlRequest.Params.Fields["filterDateOnly"].StringValue);
        Assert.Equal(V1.TypeCode.Date, executeSqlRequest.ParamTypes["filterDate"].Code);
        Assert.Equal(V1.TypeCode.Date, executeSqlRequest.ParamTypes["filterDateOnly"].Code);
    }
}
