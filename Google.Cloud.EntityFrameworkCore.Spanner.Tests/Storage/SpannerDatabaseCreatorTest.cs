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
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Storage;

#pragma warning disable EF1001 // Internal EF Core API usage.
public class SpannerDatabaseCreatorTest : IClassFixture<SpannerMockServerFixture>
{
    private readonly SpannerMockServerFixture _fixture;

    public SpannerDatabaseCreatorTest(SpannerMockServerFixture fixture)
    {
        _fixture = fixture;
        ResetMockServer();
    }

    private void ResetMockServer()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            "SELECT 1",
            StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Database not found"))));
    }

    private string ConnectionString =>
        $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

    private class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions options)
            : base(options)
        {
        }
    }

    private TestDbContext CreateDbContext()
    {
        var serviceProvider = SpannerTestHelpers.Instance.CreateServiceProvider();
        var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>()
            .UseInternalServiceProvider(serviceProvider)
            .UseSpanner(ConnectionString, options =>
            {
                SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false);
            }, ChannelCredentials.Insecure);
        return new TestDbContext(optionsBuilder.Options);
    }

    [Fact]
    public void DatabaseCreator_Exists_ReturnsFalseWhenDatabaseNotFound()
    {
        using var dbContext = CreateDbContext();
        var creator = Assert.IsAssignableFrom<RelationalDatabaseCreator>(dbContext.Database.GetService<IDatabaseCreator>());
        var exists = creator.Exists();
        Assert.False(exists);
    }

    [Fact]
    public async Task DatabaseCreator_ExistsAsync_ReturnsFalseWhenDatabaseNotFound()
    {
        using var dbContext = CreateDbContext();
        var creator = Assert.IsAssignableFrom<RelationalDatabaseCreator>(dbContext.Database.GetService<IDatabaseCreator>());
        var exists = await creator.ExistsAsync(TestContext.Current.CancellationToken);
        Assert.False(exists);
    }

    [Fact]
    public void DatabaseCreator_Exists_ReturnsTrueWhenDatabaseExists()
    {
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            "SELECT 1",
            StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>> { Tuple.Create(V1.TypeCode.Int64, "Col") },
                new List<object[]> { new object[] { 1L } }));

        using var dbContext = CreateDbContext();
        var creator = Assert.IsAssignableFrom<RelationalDatabaseCreator>(dbContext.Database.GetService<IDatabaseCreator>());
        var exists = creator.Exists();
        Assert.True(exists);
    }

    [Fact]
    public async Task DatabaseCreator_ExistsAsync_ReturnsTrueWhenDatabaseExists()
    {
        _fixture.SpannerMock.AddOrUpdateStatementResult(
            "SELECT 1",
            StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>> { Tuple.Create(V1.TypeCode.Int64, "Col") },
                new List<object[]> { new object[] { 1L } }));

        using var dbContext = CreateDbContext();
        var creator = Assert.IsAssignableFrom<RelationalDatabaseCreator>(dbContext.Database.GetService<IDatabaseCreator>());
        var exists = await creator.ExistsAsync(TestContext.Current.CancellationToken);
        Assert.True(exists);
    }
}
