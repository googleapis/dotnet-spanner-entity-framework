// Copyright 2020 Google LLC
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
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    [Collection(nameof(SpannerSampleFixture))]
    public class ScaffoldingTests
    {
        private readonly SpannerSampleFixture _fixture;

        public ScaffoldingTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [Fact]
        public async void AllTablesAreGenerated()
        {
            using (var connection = _fixture.GetConnection())
            {
                var tableNames = new string[] {
                    "Singers", "Albums", "Tracks", "Venues", "Concerts", "Performances", "TableWithAllColumnTypes"
                };
                var tables = new SpannerParameterCollection();
                tables.Add("tables", SpannerDbType.ArrayOf(SpannerDbType.String), tableNames);
                var cmd = connection.CreateSelectCommand(
                    "SELECT COUNT(*) " +
                    "FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_CATALOG='' AND TABLE_SCHEMA='' AND TABLE_NAME IN UNNEST (@tables)", tables);
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    Assert.True(await reader.ReadAsync());
                    Assert.Equal(tableNames.Length, reader.GetInt64(0));
                    Assert.False(await reader.ReadAsync());
                }
            }
        }

        [Fact]
        public async void CanInsertVenue()
        {
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var venue = new Venues
                {
                    Code = "CON",
                    Name = "Concert Hall",
                    Active = true,
                    Capacity = 2000,
                    Ratings = new List<double> {8.9, 6.5, 8.0 },
                };
                db.Venues.Add(venue);
                await db.SaveChangesAsync();

                // Reget the venue from the database.
                var refreshedVenue = await db.Venues.FindAsync("CON");
                Assert.Equal("Concert Hall", refreshedVenue.Name);
            }
        }
    }
}
