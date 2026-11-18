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
using Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Update;
using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Update
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class SpannerUpdateSqlGeneratorTest : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public SpannerUpdateSqlGeneratorTest(SpannerMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        [Theory]
        [InlineData("Singers", "`Singers`")]
        [InlineData("My`Singers", @"`My\`Singers`")]
        [InlineData(@"Singers\", @"`Singers\\`")]
        [InlineData(@"Singers\` UNION SELECT 1 --", @"`Singers\\\` UNION SELECT 1 --`")]
        [InlineData("My\nSingers", @"`My\nSingers`")]
        [InlineData("My\0Singers", @"`My\x00Singers`")]
        public void GenerateSelectConcurrencyCheckSql_ValidTableName_DelimitsIdentifier(string tableName, string expectedDelimitedTable)
        {
            using var context = SpannerTestHelpers.Instance.CreateContext();
            var generator = Assert.IsType<SpannerUpdateSqlGenerator>(context.GetService<IUpdateSqlGenerator>());

            var condition = new ColumnModification(new ColumnModificationParameters
            {
                ColumnName = "SingerId",
                OriginalValue = 1L,
                GenerateParameterName = () => "p0",
                IsCondition = true,
            });

            var sql = generator.GenerateSelectConcurrencyCheckSql(tableName, [condition]);

            Assert.Equal($"SELECT 1 FROM {expectedDelimitedTable} {Environment.NewLine}WHERE `SingerId` = @p0", sql);
        }

        [Fact]
        public void GenerateSelectConcurrencyCheckSql_MultipleAndNullConditions_EscapesColumnsAndFormatsWhereClause()
        {
            using var context = SpannerTestHelpers.Instance.CreateContext();
            var generator = Assert.IsType<SpannerUpdateSqlGenerator>(context.GetService<IUpdateSqlGenerator>());

            var keyCondition = new ColumnModification(new ColumnModificationParameters
            {
                ColumnName = @"Singer`Id\",
                OriginalValue = 1L,
                GenerateParameterName = () => "p0",
                IsCondition = true,
            });
            var versionCondition = new ColumnModification(new ColumnModificationParameters
            {
                ColumnName = "Version",
                OriginalValue = null,
                GenerateParameterName = () => "p1",
                IsCondition = true,
            });

            var sql = generator.GenerateSelectConcurrencyCheckSql("Singers", [keyCondition, versionCondition]);

            Assert.Equal(
                $"SELECT 1 FROM `Singers` {Environment.NewLine}WHERE `Singer\\`Id\\\\` = @p0 AND `Version` IS NULL",
                sql);
        }

        [Fact]
        public void GenerateSelectConcurrencyCheckSql_NullOrEmptyTable_Throws()
        {
            using var context = SpannerTestHelpers.Instance.CreateContext();
            var generator = Assert.IsType<SpannerUpdateSqlGenerator>(context.GetService<IUpdateSqlGenerator>());
            var condition = new ColumnModification(new ColumnModificationParameters
            {
                ColumnName = "SingerId",
                OriginalValue = 1L,
                GenerateParameterName = () => "p0",
                IsCondition = true,
            });

            Assert.Throws<ArgumentNullException>(() => generator.GenerateSelectConcurrencyCheckSql(null, [condition]));
            Assert.Throws<ArgumentException>(() => generator.GenerateSelectConcurrencyCheckSql(string.Empty, [condition]));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task SaveChanges_UsingMutations_DelimitsTableInConcurrencyCheckSql(bool useAsync)
        {
            var cancellationToken = TestContext.Current.CancellationToken;
            var expectedSql = $"SELECT 1 FROM `Singers_Concurrency` {Environment.NewLine}WHERE `Id` = @p0 AND `Version` = @p1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(expectedSql, StatementResult.CreateSelect1ResultSet());

            await using var context = new ConcurrencyDbContext(ConnectionString);
            var entity = new ConcurrencyEntity { Id = 1L, Name = "Original", Version = 1L };
            context.Attach(entity);
            entity.Name = "Updated";

            var updated = useAsync
                ? await context.SaveChangesAsync(cancellationToken)
                : context.SaveChanges();

            Assert.Equal(1, updated);
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
                request => Assert.Equal(expectedSql, request.Sql));
            Assert.Collection(
                _fixture.SpannerMock.Requests.OfType<CommitRequest>(),
                request =>
                {
                    var mutation = Assert.Single(request.Mutations);
                    Assert.Equal(Mutation.OperationOneofCase.Update, mutation.OperationCase);
                    Assert.Equal("Singers_Concurrency", mutation.Update.Table);
                });
        }

        private class ConcurrencyEntity
        {
            [Key]
            public long Id { get; set; }

            public string Name { get; set; }

            [ConcurrencyCheck]
            public long Version { get; set; }
        }

        private class ConcurrencyDbContext : DbContext
        {
            private readonly string _connectionString;

            public ConcurrencyDbContext(string connectionString)
            {
                _connectionString = connectionString;
            }

            public DbSet<ConcurrencyEntity> Entities { get; set; }

            protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            {
                optionsBuilder
                    .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                    .UseMutations(MutationUsage.Always);
            }

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                modelBuilder.Entity<ConcurrencyEntity>().ToTable("Singers_Concurrency");
            }
        }
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
