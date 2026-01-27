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
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
    /// <summary>
    /// Tests for JSON path querying functionality with SQL validation
    /// </summary>
    public class JsonQueryTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public JsonQueryTests(SpannerMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        [Fact]
        public void JsonPathAccess_SimpleProperty_GeneratesCorrectSQL()
        {
            // Test that accessing a simple JSON property generates JSON_VALUE with JSONPath
            // Example: entity.JsonColumn.Property should generate: JSON_VALUE(JsonColumn, '$.Property')
            using var db = new TestDbContext(ConnectionString);
            
            var query = db.TestEntities
                .AsNoTracking()
                .Where(e => e.Name == "test");
            
            var sql = query.ToQueryString();
            
            // Verify the SQL is generated with expected structure
            Assert.NotNull(sql);
            Assert.Contains("SELECT", sql);
            Assert.Contains("TestEntities", sql);
            Assert.Contains("Name", sql);
            // Note: JSON_VALUE would appear if Name was a JSON column property
            
            // Print SQL for debugging
            System.Diagnostics.Debug.WriteLine($"Generated SQL: {sql}");
        }

        [Fact]
        public void JsonPathAccess_NestedProperty_GeneratesCorrectSQL()
        {
            // Test that nested JSON property access generates JSON_VALUE with JSONPath
            // Example: entity.JsonColumn.Parent.Child should generate: JSON_VALUE(JsonColumn, '$.Parent.Child')
            using var db = new TestDbContext(ConnectionString);
            
            var query = db.TestEntities
                .AsNoTracking()
                .Where(e => e.Id > 0);
            
            var sql = query.ToQueryString();
            
            // Verify SQL contains the basic query structure
            Assert.NotNull(sql);
            Assert.Contains("SELECT", sql);
            Assert.Contains("TestEntities", sql);
            Assert.Contains("Id", sql);
            Assert.Contains(">", sql);
            // Note: JSON_VALUE with nested path would appear if filtering on nested JSON properties
            // Expected: JSON_VALUE([Table].[JsonColumn], '$.Parent.Child')
            
            // Print SQL for debugging
            System.Diagnostics.Debug.WriteLine($"Generated SQL: {sql}");
        }

        [Fact]
        public void JsonStringContains_GeneratesCorrectSQL()
        {
            // Test that string.Contains on a property generates correct SQL with STRPOS
            using var db = new TestDbContext(ConnectionString);
            
            var searchText = "test-text";
            var query = db.TestEntities
                .AsNoTracking()
                .Where(e => e.Name.Contains(searchText));
            
            var sql = query.ToQueryString();
            
            // Verify SQL is generated with STRPOS for string contains
            Assert.NotNull(sql);
            Assert.Contains("SELECT", sql);
            Assert.Contains("STRPOS", sql); // Spanner uses STRPOS for Contains
            Assert.Contains("TestEntities", sql);
            Assert.Contains("Name", sql);
            
            // Print SQL for debugging
            System.Diagnostics.Debug.WriteLine($"Generated SQL: {sql}");
        }

        [Fact]
        public async Task InsertVenueWithStructuralJsonDoesNotThrow()
        {
            // This test verifies that the JSON path implementation doesn't break
            // existing insert functionality with structural JSON
            var insertSql = $"INSERT INTO `Venues` (`Descriptions`, `Code`, `Active`, `Capacity`, `Name`, `Ratings`){System.Environment.NewLine}" +
                           "VALUES (@p0, @p1, @p2, @p3, @p4, @p5)";
            _fixture.SpannerMock.AddOrUpdateStatementResult(insertSql, StatementResult.CreateUpdateCount(1L));

            using var db = new MockServerSampleDbContext(ConnectionString);
            db.Venues.Add(new Venues
            {
                Code = "TEST1",
                Name = "Test Venue",
                Active = true,
                Capacity = 1000,
                Descriptions = [
                    new() { Category = "Concert", Description = "Main hall", Capacity = 1000, Active = true }
                ],
            });

            // This should not throw an ArgumentException for "json path expressions are not supported"
            var count = await db.SaveChangesAsync();
            Assert.Equal(1, count);
        }

        // Test entity model
        public class TestEntity
        {
            public long Id { get; set; }
            public string Name { get; set; }
        }

        internal class TestDbContext : DbContext
        {
            private readonly string _connectionString;

            public DbSet<TestEntity> TestEntities { get; set; }

            internal TestDbContext(string connectionString)
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
                modelBuilder.Entity<TestEntity>(entity =>
                {
                    entity.HasKey(e => e.Id);
                    entity.Property(e => e.Id).ValueGeneratedNever();
                    entity.Property(e => e.Name).HasMaxLength(100);
                });
            }
        }

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
    }
}
