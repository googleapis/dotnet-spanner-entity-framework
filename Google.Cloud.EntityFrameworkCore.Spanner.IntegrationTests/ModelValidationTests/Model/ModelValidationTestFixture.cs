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

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.ModelValidationTests
{
    public class ModelValidationTestFixture : SpannerFixtureBase
    {
        public ModelValidationTestFixture()
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
                    "Singers",
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
                @"CREATE TABLE Singers (
                    SingerId INT64,
                    FirstName STRING(MAX),
                    LastName STRING(MAX),
                 ) PRIMARY KEY (SingerId)",
                @"CREATE TABLE Albums (
                    SingerId INT64,
                    AlbumId INT64,
                    Title STRING(MAX),
                 ) PRIMARY KEY (SingerId, AlbumId),
                 INTERLEAVE IN PARENT Singers ON DELETE CASCADE",
                @"CREATE UNIQUE INDEX Idx_Albums_SingerTitle ON Albums (SingerId, Title)"
            ).ExecuteNonQuery();
        }
    }
}
