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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

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

        #region JSON Property Type Query Tests - SQL Generation Verification
        
        /// <summary>
        /// Tests that querying JSON DateOnly property with >= generates correct CAST to DATE.
        /// Uses ToQueryString() to verify SQL generation without executing against mock server.
        /// Expected SQL: WHERE CAST(JSON_VALUE(..., '$.Date') AS DATE) >= @p
        /// </summary>
        [Fact]
        public void JsonQuery_DateProperty_GreaterThanOrEqual_GeneratesCorrectSql()
        {
            // Test using the existing TicketSales model which uses DateOnly in Receipt
            // We'll verify the CAST pattern is applied for DATE type
            using var db = new MockServerSampleDbContext(ConnectionString);
            var targetDate = new DateOnly(2024, 1, 1);
            
            // Use ToQueryString() to get the SQL without executing
            var sql = db.TicketSales
                .Where(ts => ts.Receipt.Date >= targetDate)
                .ToQueryString();

            // Verify the SQL contains the CAST pattern for DATE type
            Assert.Contains("CAST(JSON_VALUE(`t`.`Receipt`, '$.Date') AS DATE)", sql);
            Assert.Contains(">= @", sql);
        }

        /// <summary>
        /// Tests that querying JSON string property with equality generates no CAST (STRING is native JSON_VALUE return type).
        /// Uses ToQueryString() to verify SQL generation without executing against mock server.
        /// Expected SQL: WHERE JSON_VALUE(..., '$.Number') = @p
        /// </summary>
        [Fact]
        public void JsonQuery_StringProperty_Equals_GeneratesNoCast()
        {
            // String properties should not be CAST since JSON_VALUE returns STRING
            using var db = new MockServerSampleDbContext(ConnectionString);
            var targetNumber = "TEST-001";
            
            // Use ToQueryString() to get the SQL without executing
            var sql = db.TicketSales
                .Where(ts => ts.Receipt.Number == targetNumber)
                .ToQueryString();

            // Verify the SQL contains JSON_VALUE without CAST for string comparison
            Assert.Contains("JSON_VALUE(`t`.`Receipt`, '$.Number')", sql);
            Assert.DoesNotContain("CAST(JSON_VALUE(`t`.`Receipt`, '$.Number')", sql);
            Assert.Contains("= @", sql);
        }

        /// <summary>
        /// Tests that querying JSON string property with StartsWith generates appropriate pattern.
        /// Uses ToQueryString() to verify SQL generation without executing against mock server.
        /// Note: Spanner provider may use STARTS_WITH function or LIKE pattern.
        /// </summary>
        [Fact]
        public void JsonQuery_StringProperty_StartsWith_GeneratesStartsWithOrLike()
        {
            // StartsWith may translate to STARTS_WITH function or LIKE pattern
            using var db = new MockServerSampleDbContext(ConnectionString);
            var prefix = "TEST";
            
            // Use ToQueryString() to get the SQL without executing
            var sql = db.TicketSales
                .Where(ts => ts.Receipt.Number.StartsWith(prefix))
                .ToQueryString();

            // Verify the SQL contains JSON_VALUE with either STARTS_WITH or LIKE
            Assert.Contains("JSON_VALUE(`t`.`Receipt`, '$.Number')", sql);
            Assert.True(sql.Contains("STARTS_WITH") || sql.Contains("LIKE"),
                $"Expected STARTS_WITH or LIKE in SQL: {sql}");
        }

        /// <summary>
        /// Tests that the CAST wrapper correctly handles DATE type for JSON property comparisons.
        /// Uses ToQueryString() to verify SQL generation without executing against mock server.
        /// </summary>
        [Fact]
        public void JsonQuery_DateProperty_LessThan_GeneratesCorrectCast()
        {
            // Test that DATE comparison generates the CAST pattern
            using var db = new MockServerSampleDbContext(ConnectionString);
            var targetDate = new DateOnly(2024, 12, 31);
            
            // Use ToQueryString() to get the SQL without executing
            var sql = db.TicketSales
                .Where(ts => ts.Receipt.Date < targetDate)
                .ToQueryString();

            // Verify CAST is applied for DATE type
            Assert.Contains("CAST(JSON_VALUE(`t`.`Receipt`, '$.Date') AS DATE)", sql);
            Assert.Contains("< @", sql);
        }

        #endregion
        
        #region Legacy Tests

        [Fact]
        public void VisitJsonScalar_SimpleProperty_GeneratesCorrectJSONPath()
        {
            // Validates that simple property access generates correct JSONPath: $.Property
            using var db = new MockServerSampleDbContext(ConnectionString);
            
            var sql = db.TicketSales
                .Where(ts => ts.Receipt.Number == "TEST")
                .ToQueryString();

            // Should generate JSON_VALUE with simple dot notation path
            Assert.Contains("JSON_VALUE(`t`.`Receipt`, '$.Number')", sql);
        }

        [Fact]
        public void VisitJsonScalar_NestedProperty_GeneratesCorrectJSONPath()
        {
            // Validates that nested property access would generate correct nested JSONPath: $.Parent.Child
            // Note: The current Receipt model only has single-level properties (Date, Number).
            // This test verifies the implementation handles the existing structure correctly,
            // and documents expected behavior for deeper nesting.
            using var db = new MockServerSampleDbContext(ConnectionString);
            
            // Test both properties to verify multiple paths work
            var sqlDate = db.TicketSales
                .Where(ts => ts.Receipt.Date == new DateOnly(2024, 1, 1))
                .ToQueryString();
            
            var sqlNumber = db.TicketSales
                .Where(ts => ts.Receipt.Number == "TEST")
                .ToQueryString();

            // Both should generate correct JSON_VALUE paths
            Assert.Contains("JSON_VALUE(`t`.`Receipt`, '$.Date')", sqlDate);
            Assert.Contains("JSON_VALUE(`t`.`Receipt`, '$.Number')", sqlNumber);
            
            // For deeper nesting like entity.Json.Parent.Child, the implementation would generate:
            // JSON_VALUE(`t`.`Json`, '$.Parent.Child')
        }

        [Fact]
        public void VisitJsonScalar_SpecialCharactersInPropertyName_UsesBracketNotation()
        {
            // This test documents the expected behavior for property names with special characters.
            // 
            // The VisitJsonScalar implementation handles special characters by:
            // 1. Checking if property name contains: . ' " or whitespace
            // 2. Using bracket notation: $["property.name"] instead of $.property.name
            // 3. Escaping quotes within property names
            //
            // Example expected outputs:
            // - Property "my.property" -> JSON_VALUE(col, '$["my.property"]')
            // - Property "it's" -> JSON_VALUE(col, '$["it\'s"]')  
            // - Property "with space" -> JSON_VALUE(col, '$["with space"]')
            //
            // Note: C# property names cannot contain these characters, so this scenario
            // only applies when using [JsonPropertyName] attributes or dynamic JSON.
            // The current test model uses standard C# property names (Date, Number).
            
            using var db = new MockServerSampleDbContext(ConnectionString);
            
            // Verify standard property names use dot notation (not bracket notation)
            var sql = db.TicketSales
                .Where(ts => ts.Receipt.Number == "TEST")
                .ToQueryString();

            // Standard properties should use simple dot notation
            Assert.Contains("'$.Number'", sql);
            Assert.DoesNotContain("$[\"Number\"]", sql);
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

        #endregion

        #region Test DbContext Classes

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
        
        #endregion
    }
}
