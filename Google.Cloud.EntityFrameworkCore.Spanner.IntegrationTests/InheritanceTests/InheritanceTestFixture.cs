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
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.InheritanceTests.Model;
using Google.Cloud.Spanner.Common.V1;
using Microsoft.EntityFrameworkCore;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.InheritanceTests
{
    /// <summary>
    /// DbContext for versioned tables.
    /// </summary>
    internal class TestSpannerInheritanceDbContext : SpannerInheritanceDbContext
    {
        public TestSpannerInheritanceDbContext()
        {
        }

        private readonly DatabaseName _databaseName;

        internal TestSpannerInheritanceDbContext(DatabaseName databaseName) => _databaseName = databaseName;

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

    public class InheritanceTestFixture : SpannerFixtureBase
    {
        public InheritanceTestFixture()
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
                    "Albums", "Persons",
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
                @"CREATE TABLE Persons (
                    PersonId INT64,
                    Discriminator STRING(MAX),
                    Name STRING(MAX),
                    StageName STRING(MAX),
                    WorksForPersonId INT64,
                    CONSTRAINT FK_StageWorker_WorksFor FOREIGN KEY (WorksForPersonId) REFERENCES Persons (PersonId),
                 ) PRIMARY KEY (PersonId)",
                @"CREATE TABLE Albums (
                    AlbumId INT64,
                    SingerPersonId INT64,
                    Title STRING(MAX),
                    CONSTRAINT FK_Albums_Singer FOREIGN KEY (SingerPersonId) REFERENCES Persons (PersonId),
                 ) PRIMARY KEY (AlbumId)"
            ).ExecuteNonQuery();
        }
    }
}
