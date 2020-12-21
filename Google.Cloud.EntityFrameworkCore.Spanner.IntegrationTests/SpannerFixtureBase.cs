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

using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1.Internal.Logging;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    /// <summary>
    /// Base classes for test fixtures for Spanner. Automatically creates a new database
    /// if no test database has been set using the environment variable TEST_SPANNER_DATABASE.
    /// If a database is specifically created by this fixture, the database will also
    /// automatically be dropped after the test finishes.
    ///
    /// If a value is set for TEST_SPANNER_DATABASE then that database is used. The database
    /// is not dropped after the test finishes.
    /// </summary>
    public abstract class SpannerFixtureBase : CloudProjectFixtureBase
    {
        public SpannerTestDatabase Database { get; }

        public SpannerFixtureBase()
        {
            Database = SpannerTestDatabase.GetInstance(ProjectId);
        }

        public async override void Dispose()
        {
            if (Database.Fresh)
            {
                using (var connection = new SpannerConnection(Database.NoDbConnectionString))
                {
                    var dropCommand = connection.CreateDdlCommand($"DROP DATABASE {DatabaseName.DatabaseId}");
                    await dropCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        public DatabaseName DatabaseName => Database.DatabaseName;
        public SpannerConnection GetConnection() => Database.GetConnection();
        public string ConnectionString => Database.ConnectionString;
        public SpannerConnection GetConnection(Logger logger) => Database.GetConnection(logger);
    }
}
