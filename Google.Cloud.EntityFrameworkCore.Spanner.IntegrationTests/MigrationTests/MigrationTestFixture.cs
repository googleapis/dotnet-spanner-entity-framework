using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1.Internal.Logging;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    /// <summary>
    /// DbContext for Migration tables.
    /// </summary>
    internal class TestMigrationDbContext : MigrationDbContext
    {
        private readonly DatabaseName _databaseName;

        internal TestMigrationDbContext(DatabaseName databaseName) => _databaseName = databaseName;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner($"Data Source={_databaseName}");
            }
        }
    }

    [CollectionDefinition(nameof(MigrationTestFixture))]
    public class MigrationTestFixture : SpannerFixtureBase, ICollectionFixture<MigrationTestFixture>
    {
        public MigrationTestFixture()
        {
            if (Database.Fresh)
            {
                Logger.DefaultLogger.Debug($"Generating Tables for database {Database.DatabaseName} using migration.");
                GenerateTables();
            }
            else
            {
                Logger.DefaultLogger.Debug($"Deleting data in {Database.DatabaseName}");
                ClearTables();
            }
        }

        private void ClearTables()
        {
            using (var con = GetConnection())
            {
                using (var tx = con.BeginTransaction())
                {
                    var cmd = con.CreateBatchDmlCommand();
                    cmd.Transaction = tx;
                    foreach (var table in new string[]
                    {
                        "OrderDetails",
                        "Orders",
                        "Products",
                        "Categories",
                        "AllColTypes"
                    })
                    {
                        cmd.Add($"DELETE FROM {table} WHERE TRUE");
                    }
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Generate Tables using migration.
        /// </summary>
        private void GenerateTables()
        {
            using var context = new TestMigrationDbContext(Database.DatabaseName);
            context.Database.Migrate();
        }
    }
}
