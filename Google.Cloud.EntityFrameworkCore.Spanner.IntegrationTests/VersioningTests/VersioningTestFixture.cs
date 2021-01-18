// Copyright 2021 Google LLC
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

using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.Spanner.Common.V1;
using Microsoft.EntityFrameworkCore;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    /// <summary>
    /// DbContext for versioned tables.
    /// </summary>
    internal class TestSpannerVersionDbContext : SpannerVersionDbContext
    {
        public TestSpannerVersionDbContext()
        {
        }

        private readonly DatabaseName _databaseName;

        internal TestSpannerVersionDbContext(DatabaseName databaseName) => _databaseName = databaseName;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseLazyLoadingProxies()
                    .UseSpanner($"Data Source={_databaseName}");
            }
        }
    }

    public class VersioningTestFixture : SpannerFixtureBase
    {
        public VersioningTestFixture()
        {
            if (Database.Fresh)
            {
                CreateTables();
            }
            else
            {
                ClearTables();
            }
        }

        private void ClearTables()
        {
            using var con = GetConnection();
            con.RunWithRetriableTransactionAsync((transaction) =>
            {
                var cmd = transaction.CreateBatchDmlCommand();
                foreach (var table in new string[]
                {
                    "SingersWithVersion",
                })
                {
                    cmd.Add($"DELETE FROM {table} WHERE TRUE");
                }
                return cmd.ExecuteNonQueryAsync();
            }).ResultWithUnwrappedExceptions();
        }

        private void CreateTables()
        {
            using var connection = GetConnection();
            connection.CreateDdlCommand(
                @"CREATE TABLE SingersWithVersion (
                    SingerId INT64,
                    FirstName STRING(MAX),
                    LastName STRING(MAX),
                    Version INT64,
                 ) PRIMARY KEY (SingerId)",
                @"CREATE TABLE AlbumsWithVersion (
                    SingerId INT64,
                    AlbumId INT64,
                    Title STRING(MAX),
                    Version INT64,
                 ) PRIMARY KEY (SingerId, AlbumId),
                 INTERLEAVE IN PARENT SingersWithVersion ON DELETE CASCADE"
            ).ExecuteNonQuery();
        }
    }
}
