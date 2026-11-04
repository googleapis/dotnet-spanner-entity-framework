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
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    public class JsonTypeMappingIntegrationTests : IClassFixture<SpannerSampleFixture>
    {
        private readonly SpannerSampleFixture _fixture;

        public JsonTypeMappingIntegrationTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task Can_insert_and_query_entity_with_scalar_string_json()
        {
            var rowIdentifier = _fixture.RandomLong();
            using var scalarStringJson = JsonDocument.Parse("\"hello-spanner-json-world\"");

            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                context.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes
                {
                    ColInt64 = rowIdentifier,
                    ColJson = scalarStringJson,
                });
                var affectedRows = await context.SaveChangesAsync(TestContext.Current.CancellationToken);
                Assert.Equal(1, affectedRows);
            }

            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var queryResult = await context.TableWithAllColumnTypes
                    .FirstOrDefaultAsync(row => row.ColInt64 == rowIdentifier, TestContext.Current.CancellationToken);

                Assert.NotNull(queryResult);
                Assert.NotNull(queryResult.ColJson);
                Assert.Equal(JsonValueKind.String, queryResult.ColJson.RootElement.ValueKind);
                Assert.Equal("hello-spanner-json-world", queryResult.ColJson.RootElement.GetString());
            }
        }

        [Fact]
        public async Task Can_insert_and_query_entity_with_scalar_null_json()
        {
            var rowIdentifier = _fixture.RandomLong();
            using var nullJson = JsonDocument.Parse("null");

            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                context.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes
                {
                    ColInt64 = rowIdentifier,
                    ColJson = nullJson,
                });
                var affectedRows = await context.SaveChangesAsync(TestContext.Current.CancellationToken);
                Assert.Equal(1, affectedRows);
            }

            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var queryResult = await context.TableWithAllColumnTypes
                    .FirstOrDefaultAsync(row => row.ColInt64 == rowIdentifier, TestContext.Current.CancellationToken);

                Assert.NotNull(queryResult);
                Assert.NotNull(queryResult.ColJson);
                Assert.Equal(JsonValueKind.Null, queryResult.ColJson.RootElement.ValueKind);
            }
        }

        [Fact]
        public async Task Can_insert_and_query_entity_with_scalar_string_and_null_in_json_array()
        {
            var rowIdentifier = _fixture.RandomLong();
            using var scalarStringJson = JsonDocument.Parse("\"array-string-element\"");
            using var nullJson = JsonDocument.Parse("null");
            using var objectJson = JsonDocument.Parse("{\"key\":\"value\"}");

            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                context.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes
                {
                    ColInt64 = rowIdentifier,
                    ColJsonArray = new List<JsonDocument> { scalarStringJson, nullJson, null, objectJson },
                });
                var affectedRows = await context.SaveChangesAsync(TestContext.Current.CancellationToken);
                Assert.Equal(1, affectedRows);
            }

            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var queryResult = await context.TableWithAllColumnTypes
                    .FirstOrDefaultAsync(row => row.ColInt64 == rowIdentifier, TestContext.Current.CancellationToken);

                Assert.NotNull(queryResult);
                Assert.NotNull(queryResult.ColJsonArray);
                Assert.Equal(4, queryResult.ColJsonArray.Count);

                Assert.NotNull(queryResult.ColJsonArray[0]);
                Assert.Equal(JsonValueKind.String, queryResult.ColJsonArray[0].RootElement.ValueKind);
                Assert.Equal("array-string-element", queryResult.ColJsonArray[0].RootElement.GetString());

                Assert.NotNull(queryResult.ColJsonArray[1]);
                Assert.Equal(JsonValueKind.Null, queryResult.ColJsonArray[1].RootElement.ValueKind);

                Assert.Null(queryResult.ColJsonArray[2]);

                Assert.NotNull(queryResult.ColJsonArray[3]);
                Assert.Equal("value", queryResult.ColJsonArray[3].RootElement.GetProperty("key").GetString());
            }
        }

        [Fact]
        public async Task Can_update_json_property_from_object_to_scalar_string_and_null()
        {
            var rowIdentifier = _fixture.RandomLong();
            using var initialJson = JsonDocument.Parse("{\"initial\":\"value\"}");

            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                context.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes
                {
                    ColInt64 = rowIdentifier,
                    ColJson = initialJson,
                });
                await context.SaveChangesAsync(TestContext.Current.CancellationToken);
            }

            using var updatedStringJson = JsonDocument.Parse("\"updated-scalar-string\"");
            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var rowToUpdate = await context.TableWithAllColumnTypes
                    .FirstOrDefaultAsync(row => row.ColInt64 == rowIdentifier, TestContext.Current.CancellationToken);
                Assert.NotNull(rowToUpdate);

                rowToUpdate.ColJson = updatedStringJson;
                var affectedRows = await context.SaveChangesAsync(TestContext.Current.CancellationToken);
                Assert.Equal(1, affectedRows);
            }

            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var queryResult = await context.TableWithAllColumnTypes
                    .FirstOrDefaultAsync(row => row.ColInt64 == rowIdentifier, TestContext.Current.CancellationToken);
                Assert.NotNull(queryResult);
                Assert.Equal(JsonValueKind.String, queryResult.ColJson.RootElement.ValueKind);
                Assert.Equal("updated-scalar-string", queryResult.ColJson.RootElement.GetString());
            }

            using var updatedNullJson = JsonDocument.Parse("null");
            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var rowToUpdate = await context.TableWithAllColumnTypes
                    .FirstOrDefaultAsync(row => row.ColInt64 == rowIdentifier, TestContext.Current.CancellationToken);
                Assert.NotNull(rowToUpdate);

                rowToUpdate.ColJson = updatedNullJson;
                var affectedRows = await context.SaveChangesAsync(TestContext.Current.CancellationToken);
                Assert.Equal(1, affectedRows);
            }

            await using (var context = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var queryResult = await context.TableWithAllColumnTypes
                    .FirstOrDefaultAsync(row => row.ColInt64 == rowIdentifier, TestContext.Current.CancellationToken);
                Assert.NotNull(queryResult);
                Assert.Equal(JsonValueKind.Null, queryResult.ColJson.RootElement.ValueKind);
            }
        }
    }
}
