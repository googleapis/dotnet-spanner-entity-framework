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

using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class SqlLiteralGenerationTests : IClassFixture<SpannerSampleFixture>
    {
        private readonly SpannerSampleFixture _fixture;

        public SqlLiteralGenerationTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [Theory]
        [InlineData("Simple text")]
        [InlineData("")]
        [InlineData("Marc's")]
        [InlineData("Marc's 'greatest' hits")]
        [InlineData("Marc'")]
        [InlineData("Marc''")]
        [InlineData("it'''s")]
        [InlineData(@"C:\Users\Alice\Documents\report.pdf")]
        [InlineData(@"directory\")]
        [InlineData(@"test\'")]
        [InlineData(@"path\'to\'file")]
        [InlineData("Hello, 世界! 🌍")]
        [InlineData("''' OR 1=1 --")]
        [InlineData("'''; DROP TABLE TestTable; --")]
        [InlineData("' UNION SELECT 'a', 'b' --")]
        [InlineData("Line 1\nLine 2\nLine 3")]
        [InlineData("Line 1\r\nLine 2")]
        [InlineData("test\0test")]
        public async Task String_sql_literals_execute_successfully_on_spanner(string inputValue)
        {
            var mapping = SpannerStringTypeMapping.Default;
            var sqlLiteral = mapping.GenerateSqlLiteral(inputValue);

            await using var connection = _fixture.GetConnection();
            await using var command = connection.CreateSelectCommand($"SELECT {sqlLiteral}");
            var scalarResult = (string)await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);

            Assert.Equal(inputValue, scalarResult);
        }

        [Fact]
        public async Task Json_sql_literals_from_json_document_execute_successfully_on_spanner()
        {
            var mapping = SpannerJsonTypeMapping.Default;

            var testCases = new (string JsonString, string PropertyName, string ExpectedValue)[]
            {
                ("""{"name":"Alice","age":30}""", "name", "Alice"),
                ("""{"name":"Marc's House","city":"O'Fallon"}""", "name", "Marc's House"),
                ("""{"quote":"He said \"Hello World\""}""", "quote", "He said \"Hello World\""),
                ("""{"path":"C:\\Users\\Alice"}""", "path", @"C:\Users\Alice"),
                ("""{"tags":["a","b"],"description":"It's a \\test\\ with 'quotes'"}""", "description", @"It's a \test\ with 'quotes'"),
            };

            await using var connection = _fixture.GetConnection();

            foreach (var (jsonString, propertyName, expectedValue) in testCases)
            {
                using var jsonDocument = JsonDocument.Parse(jsonString);
                var documentSqlLiteral = mapping.GenerateSqlLiteral(jsonDocument);
                var elementSqlLiteral = mapping.GenerateSqlLiteral(jsonDocument.RootElement);

                await using var documentCommand = connection.CreateSelectCommand($"SELECT JSON_VALUE({documentSqlLiteral}, '$.{propertyName}')");
                var documentExtractedValue = (string)await documentCommand.ExecuteScalarAsync(TestContext.Current.CancellationToken);
                Assert.Equal(expectedValue, documentExtractedValue);

                await using var elementCommand = connection.CreateSelectCommand($"SELECT JSON_VALUE({elementSqlLiteral}, '$.{propertyName}')");
                var elementExtractedValue = (string)await elementCommand.ExecuteScalarAsync(TestContext.Current.CancellationToken);
                Assert.Equal(expectedValue, elementExtractedValue);
            }
        }

        [Theory]
        [InlineData('a', "a")]
        [InlineData('\'', "'")]
        [InlineData('\\', @"\")]
        [InlineData('\r', "\r")]
        [InlineData('\n', "\n")]
        [InlineData('\0', "\0")]
        [InlineData('"', "\"")]
        public async Task Char_sql_literals_execute_successfully_on_spanner(char inputChar, string expectedString)
        {
            var mapping = SpannerCharTypeMapping.Default;
            var sqlLiteral = mapping.GenerateSqlLiteral(inputChar);

            await using var connection = _fixture.GetConnection();
            await using var command = connection.CreateSelectCommand($"SELECT {sqlLiteral}");
            var scalarResult = (string)await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);

            Assert.Equal(expectedString, scalarResult);
        }

        [Fact]
        public async Task Json_sql_literals_from_string_execute_successfully_on_spanner()
        {
            var mapping = SpannerJsonTypeMapping.Default;

            var testCases = new (string JsonString, string PropertyName, string ExpectedValue)[]
            {
                ("""{"name":"Alice"}""", "name", "Alice"),
                ("""{"name":"Marc's"}""", "name", "Marc's"),
                ("""{"path":"C:\\Users\\Alice"}""", "path", @"C:\Users\Alice"),
                ("""{"quote":"He said \"Hi\""}""", "quote", "He said \"Hi\""),
                ("{\n  \"name\": \"Alice\"\n}", "name", "Alice"),
                ("{\r\n  \"name\": \"Alice\"\r\n}", "name", "Alice"),
                ("{\n\t\"name\": \"Alice\"\n}", "name", "Alice"),
                ("{\"name\":\"Alice\0Bob\"}", "name", "Alice\0Bob"),
                ("""{"name":"Alice\u0000Bob"}""", "name", "Alice\0Bob"),
                ("""{"key":"'; DROP TABLE Users; --"}""", "key", "'; DROP TABLE Users; --"),
            };

            await using var connection = _fixture.GetConnection();

            foreach (var (jsonString, propertyName, expectedValue) in testCases)
            {
                var sqlLiteral = mapping.GenerateSqlLiteral(jsonString);

                await using var command = connection.CreateSelectCommand($"SELECT JSON_VALUE({sqlLiteral}, '$.{propertyName}')");
                var extractedValue = (string)await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);

                Assert.Equal(expectedValue, extractedValue);
            }
        }

        [Fact]
        public async Task Structural_json_sql_literals_execute_successfully_on_spanner()
        {
            var mapping = SpannerStructuralJsonTypeMapping.Default;

            var testCases = new (string JsonString, string PropertyName, string ExpectedValue)[]
            {
                ("""{"name":"Alice"}""", "name", "Alice"),
                ("""{"title":"Marc's Album"}""", "title", "Marc's Album"),
                ("""{"quote":"\"Total Junk\""}""", "quote", "\"Total Junk\""),
                ("""{"folder":"C:\\Data"}""", "folder", @"C:\Data"),
                ("{\n\t\"name\": \"Alice\"\n}", "name", "Alice"),
                ("""{"key":"'; DROP TABLE Users; --"}""", "key", "'; DROP TABLE Users; --"),
            };

            await using var connection = _fixture.GetConnection();

            foreach (var (jsonString, propertyName, expectedValue) in testCases)
            {
                var sqlLiteral = mapping.GenerateSqlLiteral(jsonString);

                await using var command = connection.CreateSelectCommand($"SELECT JSON_VALUE({sqlLiteral}, '$.{propertyName}')");
                var extractedValue = (string)await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);

                Assert.Equal(expectedValue, extractedValue);
            }
        }

        [Fact]
        public async Task Can_insert_and_query_row_using_generated_sql_literals()
        {
            var rowIdentifier = _fixture.RandomLong();
            var stringValue = @"C:\Users\Alice\it's\a\'test'\\\";
            var stringLiteral = SpannerStringTypeMapping.Default.GenerateSqlLiteral(stringValue);

            var rawJson = """{"path":"C:\\Users\\Alice","name":"Marc's 'Special' Record"}""";
            using var jsonDocument = JsonDocument.Parse(rawJson);
            var jsonLiteral = SpannerJsonTypeMapping.Default.GenerateSqlLiteral(jsonDocument);

            var insertSql = $"INSERT INTO TableWithAllColumnTypes (ColInt64, ColString, ColJson) VALUES ({rowIdentifier}, {stringLiteral}, {jsonLiteral})";

            await using var connection = _fixture.GetConnection();
            await connection.RunWithRetriableTransactionAsync(async (transaction) =>
            {
                await using var insertCommand = connection.CreateDmlCommand(insertSql);
                insertCommand.Transaction = transaction;
                var affectedRows = await insertCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
                Assert.Equal(1, affectedRows);
            }, TestContext.Current.CancellationToken);

            using var database = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var queryResult = await database.TableWithAllColumnTypes
                .FirstOrDefaultAsync(row => row.ColInt64 == rowIdentifier, TestContext.Current.CancellationToken);

            Assert.NotNull(queryResult);
            Assert.Equal(stringValue, queryResult.ColString);
            Assert.NotNull(queryResult.ColJson);
            Assert.Equal(@"C:\Users\Alice", queryResult.ColJson.RootElement.GetProperty("path").GetString());
            Assert.Equal("Marc's 'Special' Record", queryResult.ColJson.RootElement.GetProperty("name").GetString());
        }

        [Fact]
        public async Task Can_insert_and_query_structural_json_using_generated_sql_literals()
        {
            var ticketSaleIdentifier = _fixture.RandomLong();
            var customerName = @"Marc's ""VIP"" Customer \";
            var customerNameLiteral = SpannerStringTypeMapping.Default.GenerateSqlLiteral(customerName);

            var receiptJson = """{"Date":"2026-10-14","Number":"RCPT-Marc's-\"Special\"-001"}""";
            var receiptJsonLiteral = SpannerStructuralJsonTypeMapping.Default.GenerateSqlLiteral(receiptJson);

            var insertSql = $"INSERT INTO TicketSales (Id, CustomerName, Receipt) VALUES ({ticketSaleIdentifier}, {customerNameLiteral}, {receiptJsonLiteral})";

            await using var connection = _fixture.GetConnection();
            await connection.RunWithRetriableTransactionAsync(async (transaction) =>
            {
                await using var insertCommand = connection.CreateDmlCommand(insertSql);
                insertCommand.Transaction = transaction;
                var affectedRows = await insertCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
                Assert.Equal(1, affectedRows);
            }, TestContext.Current.CancellationToken);

            using var database = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var queryResult = await database.TicketSales
                .FirstOrDefaultAsync(sale => sale.Id == ticketSaleIdentifier, TestContext.Current.CancellationToken);

            Assert.NotNull(queryResult);
            Assert.Equal(customerName, queryResult.CustomerName);
            Assert.NotNull(queryResult.Receipt);
            Assert.Equal("RCPT-Marc's-\"Special\"-001", queryResult.Receipt.Number);
            Assert.Equal(new DateOnly(2026, 10, 14), queryResult.Receipt.Date);
        }

        [Fact]
        public async Task Can_roundtrip_entities_with_special_characters_via_ef_core()
        {
            var rowIdentifier = _fixture.RandomLong();
            var specialString = @"C:\Program Files\Spanner\it's\a\'test'\\\";
            var specialJson = JsonDocument.Parse("""{"path":"C:\\Program Files\\Spanner","comment":"It's \"quoted\" and has \\backslashes\\"}""");

            using (var database = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                database.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes
                {
                    ColInt64 = rowIdentifier,
                    ColString = specialString,
                    ColJson = specialJson,
                });
                await database.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            using (var database = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var queryResult = await database.TableWithAllColumnTypes
                    .Where(row => row.ColInt64 == rowIdentifier && row.ColString == specialString)
                    .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

                Assert.NotNull(queryResult);
                Assert.Equal(specialString, queryResult.ColString);
                Assert.NotNull(queryResult.ColJson);
                Assert.Equal(@"C:\Program Files\Spanner", queryResult.ColJson.RootElement.GetProperty("path").GetString());
                Assert.Equal("It's \"quoted\" and has \\backslashes\\", queryResult.ColJson.RootElement.GetProperty("comment").GetString());
            }
        }

        [Fact]
        public async Task Can_generate_valid_sql_literal_for_string_mapped_to_json_column()
        {
            var jsonAsStringMapping = SpannerJsonStringTypeMapping.Default;
            var rawJson = """{"message":"Marc's \\special\\ test"}""";
            var sqlLiteral = jsonAsStringMapping.GenerateSqlLiteral(rawJson);

            Assert.StartsWith("JSON '", sqlLiteral);

            await using var connection = _fixture.GetConnection();
            await using var command = connection.CreateSelectCommand($"SELECT JSON_VALUE({sqlLiteral}, '$.message')");
            var extractedValue = (string)await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);

            Assert.Equal(@"Marc's \special\ test", extractedValue);
        }

        [Fact]
        public async Task Can_filter_with_inlined_constant_contains_collection()
        {
            var rowIdentifier = _fixture.RandomLong();
            var targetString = @"C:\Program Files\Spanner\it's\a\'test'\\\";

            using (var database = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                database.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes
                {
                    ColInt64 = rowIdentifier,
                    ColString = targetString,
                });
                await database.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            var searchCandidates = new[]
            {
                "''' OR 1=1 --",
                @"path\with\'quotes'",
                targetString,
                "another'value",
            };

            using (var database = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var queryResult = await database.TableWithAllColumnTypes
                    .Where(row => row.ColInt64 == rowIdentifier && searchCandidates.Contains(row.ColString))
                    .FirstOrDefaultAsync(TestContext.Current.CancellationToken);

                Assert.NotNull(queryResult);
            }
        }
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
