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
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.ShadowPropertiesModel;
using Google.Cloud.Spanner.Common.V1;
using Microsoft.EntityFrameworkCore;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.ShadowPropertiesTests
{
    /// <summary>
    /// DbContext for tables with shadow properties.
    /// </summary>
    internal class TestSpannerShadowPropertiesDbContext : SpannerShadowPropertiesDbContext
    {
        public TestSpannerShadowPropertiesDbContext()
        {
        }

        private readonly DatabaseName _databaseName;

        internal TestSpannerShadowPropertiesDbContext(DatabaseName databaseName) => _databaseName = databaseName;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseLazyLoadingProxies()
                    .UseSpanner($"Data Source={_databaseName};emulatordetection=EmulatorOrProduction");
            }
        }
    }

    public class ShadowPropertiesTestFixture : SpannerFixtureBase
    {
        public ShadowPropertiesTestFixture()
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
                foreach (var table in new string[] { "Albums", "Singers" })
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
                @"CREATE TABLE Singers (
                    SingerId INT64,
                    Name STRING(MAX),
                    LastModified TIMESTAMP OPTIONS (allow_commit_timestamp = true),
                 ) PRIMARY KEY (SingerId)",
                @"CREATE TABLE Albums (
                    AlbumId INT64,
                    Title STRING(MAX),
                    SingerId INT64,
                    LastModified TIMESTAMP OPTIONS (allow_commit_timestamp = true),
                    CONSTRAINT FK_Albums_Singers FOREIGN KEY (SingerId) REFERENCES Singers (SingerId),
                 ) PRIMARY KEY (AlbumId)"
            ).ExecuteNonQuery();
        }
    }
}
