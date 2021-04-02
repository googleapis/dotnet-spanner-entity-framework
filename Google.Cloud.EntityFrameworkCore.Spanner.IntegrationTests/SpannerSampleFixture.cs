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

using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1.Internal.Logging;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using System;
using System.IO;
using System.Reflection;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    /// <summary>
    /// DbContext for the generated sample database.
    /// </summary>
    internal class TestSpannerSampleDbContext : SpannerSampleDbContext
    {
        private readonly DatabaseName _databaseName;

        internal TestSpannerSampleDbContext(DatabaseName databaseName) => _databaseName = databaseName;

        internal TestSpannerSampleDbContext(DbContextOptions<SpannerSampleDbContext> options)
            : base(options)
        {
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner($"Data Source={_databaseName};emulatordetection=EmulatorOrProduction")
                    .UseLazyLoadingProxies();
            }
        }
    }

    internal class FullNameGenerator : ValueGenerator<string>
    {
        public override bool GeneratesTemporaryValues => false;

        public override string Next(EntityEntry entry)
        {
            var singer = entry.Entity as Singers;
            return (singer.FirstName ?? "") + " " + (singer.LastName ?? "");
        }
    }

    /// <summary>
    /// Base classes for test fixtures using the sample data model.
    /// If TEST_SPANNER_DATABASE is set to an existing database, that database will be used and the
    /// fixture assumes that the database already contains the sample data model. Any data in the
    /// existing database will be deleted.
    /// 
    /// Otherwise a new database with the sample data model is automatically created and used. The
    /// generated database is dropped when the fixture is disposed.
    /// </summary>
    public class SpannerSampleFixture : SpannerFixtureBase
    {
        public SpannerSampleFixture()
        {
            if (Database.Fresh)
            {
                Logger.DefaultLogger.Debug($"Creating database {Database.DatabaseName}");
                CreateTables();
            }
            else
            {
                Logger.DefaultLogger.Debug($"Deleting data in {Database.DatabaseName}");
                ClearTables();
            }
            Logger.DefaultLogger.Debug($"Ready to run tests");
        }

        private void ClearTables()
        {
            using var con = GetConnection();
            con.RunWithRetriableTransactionAsync((transaction) =>
            {
                var cmd = transaction.CreateBatchDmlCommand();
                foreach (var table in new string[]
                {
                    "TableWithAllColumnTypes",
                    "Performances",
                    "Concerts",
                    "Venues",
                    "Tracks",
                    "Albums",
                    "Singers",
                })
                {
                    cmd.Add($"DELETE FROM {table} WHERE TRUE");
                }
                return cmd.ExecuteNonQueryAsync();
            }).ResultWithUnwrappedExceptions();
        }

        /// <summary>
        /// Creates the sample data model. This method is only called when a new database has been
        /// created.
        /// </summary>
        private void CreateTables()
        {
            var dirPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            // We must use a slightly edited sample data model for the emulator, as the emulator does not support:
            // 1. NUMERIC data type.
            // 2. Computed columns.
            // 3. Check constraints.
            var sampleModel = IsEmulator ? "SampleDataModel - Emulator.sql" : "SampleDataModel.sql";
            var fileName = Path.Combine(dirPath, sampleModel);
            var script = File.ReadAllText(fileName);
            var statements = script.Split(";");
            for (var i = 0; i < statements.Length; i++)
            {
                statements[i] = statements[i].Trim(new char[] { '\r', '\n' });
            }
            int length = statements.Length;
            if (statements[length - 1] == "")
            {
                length--;
            }
            ExecuteDdl(statements, length);
        }

        private void ExecuteDdl(string[] ddl, int length)
        {
            string[] extraStatements = new string[length - 1];
            Array.Copy(ddl, 1, extraStatements, 0, extraStatements.Length);
            using var connection = GetConnection();
            connection.CreateDdlCommand(ddl[0].Trim(), extraStatements).ExecuteNonQuery();
        }
    }
}
