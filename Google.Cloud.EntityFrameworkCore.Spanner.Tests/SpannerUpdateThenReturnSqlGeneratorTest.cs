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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

#pragma warning disable EF1001

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests;

public class SpannerUpdateThenReturnSqlGeneratorTest : IClassFixture<SpannerMockServerFixture>
{
    private readonly SpannerMockServerFixture _fixture;

    public SpannerUpdateThenReturnSqlGeneratorTest(SpannerMockServerFixture fixture)
    {
        _fixture = fixture;
        _fixture.SpannerMock.Reset();
    }

    private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

    internal class MixedBatchDbContext : DbContext
    {
        private readonly string _connectionString;

        public MixedBatchDbContext(string connectionString)
        {
            _connectionString = connectionString;
        }

        public DbSet<EntityWithGeneratedKey> EntitiesWithGeneratedKey { get; set; }
        public DbSet<EntityWithoutGeneratedKey> EntitiesWithoutGeneratedKey { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder
                .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                .UseMutations(MutationUsage.Never);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<EntityWithGeneratedKey>(entity =>
            {
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).ValueGeneratedOnAdd();
            });
            modelBuilder.Entity<EntityWithoutGeneratedKey>(entity =>
            {
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).ValueGeneratedNever();
            });
        }
    }

    public class EntityWithGeneratedKey
    {
        public long Id { get; set; }
        public string Name { get; set; }
    }

    public class EntityWithoutGeneratedKey
    {
        public long Id { get; set; }
        public string Name { get; set; }
    }

    private static string InsertWithGenSql =>
        $"INSERT INTO `EntitiesWithGeneratedKey` (`Name`){Environment.NewLine}" +
        $"VALUES (@p0){Environment.NewLine}" +
        $"THEN RETURN `Id`{Environment.NewLine}";

    private static string InsertWithoutGenSql =>
        $"INSERT INTO `EntitiesWithoutGeneratedKey` (`Id`, `Name`){Environment.NewLine}" +
        $"VALUES (@p1, @p2)";

    private static string UpdateWithoutGenSql =>
        $"UPDATE `EntitiesWithoutGeneratedKey` SET `Name` = @p2{Environment.NewLine}" +
        $"WHERE `Id` = @p3{Environment.NewLine}";

    private static string DeleteWithoutGenSql =>
        $"DELETE FROM `EntitiesWithoutGeneratedKey`{Environment.NewLine}" +
        $"WHERE `Id` = @p1{Environment.NewLine}";

    [Fact]
    public async Task MixedBatchWithReadAndNonReadInserts_Async()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id", "12345"));
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithoutGenSql, StatementResult.CreateUpdateCount(1L));

        await using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        var nonGeneratedEntity = new EntityWithoutGeneratedKey { Id = 1L, Name = "NonGen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);
        db.EntitiesWithoutGeneratedKey.Add(nonGeneratedEntity);

        var rowsAffected = await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rowsAffected);
        Assert.Equal(12345L, generatedEntity.Id);
        Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request => Assert.Equal(InsertWithGenSql, request.Sql),
            request => Assert.Equal(InsertWithoutGenSql, request.Sql)
        );
        Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);
    }

    [Fact]
    public void MixedBatchWithReadAndNonReadInserts_Sync()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id", "12345"));
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithoutGenSql, StatementResult.CreateUpdateCount(1L));

        using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        var nonGeneratedEntity = new EntityWithoutGeneratedKey { Id = 1L, Name = "NonGen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);
        db.EntitiesWithoutGeneratedKey.Add(nonGeneratedEntity);

        var rowsAffected = db.SaveChanges();
        Assert.Equal(2, rowsAffected);
        Assert.Equal(12345L, generatedEntity.Id);
        Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request => Assert.Equal(InsertWithGenSql, request.Sql),
            request => Assert.Equal(InsertWithoutGenSql, request.Sql)
        );
        Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);
    }

    [Fact]
    public async Task MixedBatchWithInsertUpdateDelete_Async()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id", "12345"));
        _fixture.SpannerMock.AddOrUpdateStatementResult(UpdateWithoutGenSql, StatementResult.CreateUpdateCount(1L));
        _fixture.SpannerMock.AddOrUpdateStatementResult(DeleteWithoutGenSql, StatementResult.CreateUpdateCount(1L));

        await using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);

        var existingEntityToUpdate = new EntityWithoutGeneratedKey { Id = 1L, Name = "BeforeUpdate" };
        db.Attach(existingEntityToUpdate);
        existingEntityToUpdate.Name = "AfterUpdate";

        var existingEntityToDelete = new EntityWithoutGeneratedKey { Id = 2L, Name = "ToDelete" };
        db.Attach(existingEntityToDelete);
        db.EntitiesWithoutGeneratedKey.Remove(existingEntityToDelete);

        var rowsAffected = await db.SaveChangesAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, rowsAffected);
        Assert.Equal(12345L, generatedEntity.Id);
        Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request => Assert.Equal(InsertWithGenSql, request.Sql),
            request => Assert.Equal(DeleteWithoutGenSql, request.Sql),
            request => Assert.Equal(UpdateWithoutGenSql, request.Sql)
        );
        Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);
    }

    [Fact]
    public void MixedBatchWithInsertUpdateDelete_Sync()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id", "12345"));
        _fixture.SpannerMock.AddOrUpdateStatementResult(UpdateWithoutGenSql, StatementResult.CreateUpdateCount(1L));
        _fixture.SpannerMock.AddOrUpdateStatementResult(DeleteWithoutGenSql, StatementResult.CreateUpdateCount(1L));

        using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);

        var existingEntityToUpdate = new EntityWithoutGeneratedKey { Id = 1L, Name = "BeforeUpdate" };
        db.Attach(existingEntityToUpdate);
        existingEntityToUpdate.Name = "AfterUpdate";

        var existingEntityToDelete = new EntityWithoutGeneratedKey { Id = 2L, Name = "ToDelete" };
        db.Attach(existingEntityToDelete);
        db.EntitiesWithoutGeneratedKey.Remove(existingEntityToDelete);

        var rowsAffected = db.SaveChanges();
        Assert.Equal(3, rowsAffected);
        Assert.Equal(12345L, generatedEntity.Id);
        Assert.Empty(_fixture.SpannerMock.Requests.OfType<ExecuteBatchDmlRequest>());
        Assert.Collection(
            _fixture.SpannerMock.Requests.OfType<ExecuteSqlRequest>(),
            request => Assert.Equal(InsertWithGenSql, request.Sql),
            request => Assert.Equal(DeleteWithoutGenSql, request.Sql),
            request => Assert.Equal(UpdateWithoutGenSql, request.Sql)
        );
        Assert.Single(_fixture.SpannerMock.Requests, request => request is CommitRequest);
    }

    [Fact]
    public async Task MixedBatch_ThrowsConcurrencyException_WhenReadCommandAffectsZeroRows_Async()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateResultSet(new List<Tuple<V1.Type, string>> { Tuple.Create(new V1.Type { Code = V1.TypeCode.Int64 }, "Id") }, new List<object[]>()));
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithoutGenSql, StatementResult.CreateUpdateCount(1L));

        await using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        var nonGeneratedEntity = new EntityWithoutGeneratedKey { Id = 1L, Name = "NonGen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);
        db.EntitiesWithoutGeneratedKey.Add(nonGeneratedEntity);

        var exception = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync(TestContext.Current.CancellationToken));
        var failedEntry = Assert.Single(exception.Entries);
        Assert.Same(generatedEntity, failedEntry.Entity);
    }

    [Fact]
    public void MixedBatch_ThrowsConcurrencyException_WhenReadCommandAffectsZeroRows_Sync()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateResultSet(new List<Tuple<V1.Type, string>> { Tuple.Create(new V1.Type { Code = V1.TypeCode.Int64 }, "Id") }, new List<object[]>()));
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithoutGenSql, StatementResult.CreateUpdateCount(1L));

        using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        var nonGeneratedEntity = new EntityWithoutGeneratedKey { Id = 1L, Name = "NonGen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);
        db.EntitiesWithoutGeneratedKey.Add(nonGeneratedEntity);

        var exception = Assert.Throws<DbUpdateConcurrencyException>(() => db.SaveChanges());
        var failedEntry = Assert.Single(exception.Entries);
        Assert.Same(generatedEntity, failedEntry.Entity);
    }

    [Fact]
    public async Task MixedBatch_ThrowsConcurrencyException_WhenNonReadCommandAffectsZeroRows_Async()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id", "12345"));
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithoutGenSql, StatementResult.CreateUpdateCount(0L));

        await using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        var nonGeneratedEntity = new EntityWithoutGeneratedKey { Id = 1L, Name = "NonGen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);
        db.EntitiesWithoutGeneratedKey.Add(nonGeneratedEntity);

        var exception = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync(TestContext.Current.CancellationToken));
        var failedEntry = Assert.Single(exception.Entries);
        Assert.Same(nonGeneratedEntity, failedEntry.Entity);
    }

    [Fact]
    public void MixedBatch_ThrowsConcurrencyException_WhenNonReadCommandAffectsZeroRows_Sync()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id", "12345"));
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithoutGenSql, StatementResult.CreateUpdateCount(0L));

        using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        var nonGeneratedEntity = new EntityWithoutGeneratedKey { Id = 1L, Name = "NonGen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);
        db.EntitiesWithoutGeneratedKey.Add(nonGeneratedEntity);

        var exception = Assert.Throws<DbUpdateConcurrencyException>(() => db.SaveChanges());
        var failedEntry = Assert.Single(exception.Entries);
        Assert.Same(nonGeneratedEntity, failedEntry.Entity);
    }

    [Fact]
    public async Task MixedBatch_ThrowsConcurrencyException_WhenUpdateAffectsZeroRows_Async()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id", "12345"));
        _fixture.SpannerMock.AddOrUpdateStatementResult(DeleteWithoutGenSql, StatementResult.CreateUpdateCount(1L));
        _fixture.SpannerMock.AddOrUpdateStatementResult(UpdateWithoutGenSql, StatementResult.CreateUpdateCount(0L));

        await using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);

        var existingEntityToUpdate = new EntityWithoutGeneratedKey { Id = 1L, Name = "BeforeUpdate" };
        db.Attach(existingEntityToUpdate);
        existingEntityToUpdate.Name = "AfterUpdate";

        var existingEntityToDelete = new EntityWithoutGeneratedKey { Id = 2L, Name = "ToDelete" };
        db.Attach(existingEntityToDelete);
        db.EntitiesWithoutGeneratedKey.Remove(existingEntityToDelete);

        var exception = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync(TestContext.Current.CancellationToken));
        var failedEntry = Assert.Single(exception.Entries);
        Assert.Same(existingEntityToUpdate, failedEntry.Entity);
    }

    [Fact]
    public void MixedBatch_ThrowsConcurrencyException_WhenUpdateAffectsZeroRows_Sync()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id", "12345"));
        _fixture.SpannerMock.AddOrUpdateStatementResult(DeleteWithoutGenSql, StatementResult.CreateUpdateCount(1L));
        _fixture.SpannerMock.AddOrUpdateStatementResult(UpdateWithoutGenSql, StatementResult.CreateUpdateCount(0L));

        using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);

        var existingEntityToUpdate = new EntityWithoutGeneratedKey { Id = 1L, Name = "BeforeUpdate" };
        db.Attach(existingEntityToUpdate);
        existingEntityToUpdate.Name = "AfterUpdate";

        var existingEntityToDelete = new EntityWithoutGeneratedKey { Id = 2L, Name = "ToDelete" };
        db.Attach(existingEntityToDelete);
        db.EntitiesWithoutGeneratedKey.Remove(existingEntityToDelete);

        var exception = Assert.Throws<DbUpdateConcurrencyException>(() => db.SaveChanges());
        var failedEntry = Assert.Single(exception.Entries);
        Assert.Same(existingEntityToUpdate, failedEntry.Entity);
    }

    [Fact]
    public async Task MixedBatch_ThrowsConcurrencyException_WhenDeleteAffectsZeroRows_Async()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id", "12345"));
        _fixture.SpannerMock.AddOrUpdateStatementResult(DeleteWithoutGenSql, StatementResult.CreateUpdateCount(0L));
        _fixture.SpannerMock.AddOrUpdateStatementResult(UpdateWithoutGenSql, StatementResult.CreateUpdateCount(1L));

        await using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);

        var existingEntityToUpdate = new EntityWithoutGeneratedKey { Id = 1L, Name = "BeforeUpdate" };
        db.Attach(existingEntityToUpdate);
        existingEntityToUpdate.Name = "AfterUpdate";

        var existingEntityToDelete = new EntityWithoutGeneratedKey { Id = 2L, Name = "ToDelete" };
        db.Attach(existingEntityToDelete);
        db.EntitiesWithoutGeneratedKey.Remove(existingEntityToDelete);

        var exception = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync(TestContext.Current.CancellationToken));
        var failedEntry = Assert.Single(exception.Entries);
        Assert.Same(existingEntityToDelete, failedEntry.Entity);
    }

    [Fact]
    public void MixedBatch_ThrowsConcurrencyException_WhenDeleteAffectsZeroRows_Sync()
    {
        _fixture.SpannerMock.Reset();
        _fixture.SpannerMock.AddOrUpdateStatementResult(InsertWithGenSql,
            StatementResult.CreateSingleColumnResultSet(1L, new V1.Type { Code = V1.TypeCode.Int64 }, "Id", "12345"));
        _fixture.SpannerMock.AddOrUpdateStatementResult(DeleteWithoutGenSql, StatementResult.CreateUpdateCount(0L));
        _fixture.SpannerMock.AddOrUpdateStatementResult(UpdateWithoutGenSql, StatementResult.CreateUpdateCount(1L));

        using var db = new MixedBatchDbContext(ConnectionString);

        var generatedEntity = new EntityWithGeneratedKey { Name = "Gen" };
        db.EntitiesWithGeneratedKey.Add(generatedEntity);

        var existingEntityToUpdate = new EntityWithoutGeneratedKey { Id = 1L, Name = "BeforeUpdate" };
        db.Attach(existingEntityToUpdate);
        existingEntityToUpdate.Name = "AfterUpdate";

        var existingEntityToDelete = new EntityWithoutGeneratedKey { Id = 2L, Name = "ToDelete" };
        db.Attach(existingEntityToDelete);
        db.EntitiesWithoutGeneratedKey.Remove(existingEntityToDelete);

        var exception = Assert.Throws<DbUpdateConcurrencyException>(() => db.SaveChanges());
        var failedEntry = Assert.Single(exception.Entries);
        Assert.Same(existingEntityToDelete, failedEntry.Entity);
    }

    [Fact]
    public void AppendReturningClause_DoesNotAppend_WhenNoOperations()
    {
        var dependencies = SpannerTestHelpers.Instance.CreateContextServices().GetRequiredService<UpdateSqlGeneratorDependencies>();
        var generator = new TestSpannerUpdateThenReturnSqlGenerator(dependencies);

        var commandStringBuilder = new StringBuilder();
        generator.TestAppendReturningClause(commandStringBuilder, new List<IColumnModification>(), "1");
        Assert.Equal(string.Empty, commandStringBuilder.ToString());
    }

    [Fact]
    public void AppendReturningClause_AppendsCustomAdditionalValues_WhenOperationsPresent()
    {
        var dependencies = SpannerTestHelpers.Instance.CreateContextServices().GetRequiredService<UpdateSqlGeneratorDependencies>();
        var generator = new TestSpannerUpdateThenReturnSqlGenerator(dependencies);

        var columnModification = new TestColumnModification { ColumnName = "Id" };

        var commandStringBuilder = new StringBuilder();
        generator.TestAppendReturningClause(commandStringBuilder, new List<IColumnModification> { columnModification }, "custom_token_check");
        Assert.Equal($"{Environment.NewLine}THEN RETURN `Id`, custom_token_check", commandStringBuilder.ToString());
    }

    private class TestColumnModification : IColumnModification
    {
        public IUpdateEntry Entry => throw new NotImplementedException();
        public IProperty Property => throw new NotImplementedException();
        public IColumnBase Column => throw new NotImplementedException();
        public string ColumnType => throw new NotImplementedException();
        public bool? IsNullable => throw new NotImplementedException();
        public bool UseParameter => throw new NotImplementedException();
        public bool IsRead { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool IsWrite { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool IsCondition { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool IsKey { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool IsParameter => throw new NotImplementedException();
        public string ParameterName => throw new NotImplementedException();
        public string OriginalParameterName => throw new NotImplementedException();
        public string ColumnName { get; set; } = "Id";
        public object Value { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public object OriginalValue { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public RelationalTypeMapping TypeMapping { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
        public bool UseCurrentValue => throw new NotImplementedException();
        public bool UseOriginalValue => throw new NotImplementedException();
        public bool UseCurrentValueParameter => throw new NotImplementedException();
        public bool UseOriginalValueParameter => throw new NotImplementedException();
        public void AddEntry(IUpdateEntry entry, bool write) => throw new NotImplementedException();
        public void ResetEntry(IUpdateEntry entry) => throw new NotImplementedException();
        public void AddSharedColumnModification(IColumnModification modification) => throw new NotImplementedException();
        public void ResetParameterNames() => throw new NotImplementedException();
        public string JsonPath => throw new NotImplementedException();
    }

    private class TestSpannerUpdateThenReturnSqlGenerator : SpannerUpdateThenReturnSqlGenerator
    {
        public TestSpannerUpdateThenReturnSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
            : base(dependencies)
        {
        }

        public void TestAppendReturningClause(StringBuilder commandStringBuilder, IReadOnlyList<IColumnModification> operations, string additionalValues = null)
            => AppendReturningClause(commandStringBuilder, operations, additionalValues);
    }
}
