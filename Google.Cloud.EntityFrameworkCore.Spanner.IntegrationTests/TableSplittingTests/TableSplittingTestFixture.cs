// Copyright 2021 Google Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.TableSplittingTests.Models;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1.Internal.Logging;
using Microsoft.EntityFrameworkCore;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.TableSplittingTests
{
    internal class TestTableSplittingDbContext : TableSplittingContext
    {
        public TestTableSplittingDbContext()
        {
        }

        private readonly DatabaseName _databaseName;

        internal TestTableSplittingDbContext(DatabaseName databaseName) => _databaseName = databaseName;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner($"Data Source={_databaseName};emulatordetection=EmulatorOrProduction");
            }
        }
    }

    public class TableSplittingTestFixture : SpannerFixtureBase
    {
        public TableSplittingTestFixture()
        {
            Logger.DefaultLogger.Debug($"Applying pending migration for database {Database.DatabaseName} using migration.");
            ApplyMigration();
            if (!Database.Fresh)
            {
                Logger.DefaultLogger.Debug($"Deleting data in {Database.DatabaseName}");
                ClearTables();
            }
        }

        private void ClearTables()
        {
            using var con = GetConnection();
            con.RunWithRetriableTransactionAsync((transaction) =>
            {
                var cmd = transaction.CreateBatchDmlCommand();
                cmd.Add($"DELETE FROM Orders WHERE TRUE");
                return cmd.ExecuteNonQueryAsync();
            }).ResultWithUnwrappedExceptions();
        }

        /// <summary>
        /// Applies all Pending migrations.
        /// </summary>
        private void ApplyMigration()
        {
            using var context = new TestTableSplittingDbContext(Database.DatabaseName);
            context.Database.Migrate();
        }
    }
}
