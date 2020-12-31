using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1.Internal.Logging;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    public partial class TestMigrationDbContext : MigrationDbContext
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

    [CollectionDefinition(nameof(SpannerMigrationFixture))]
    public class SpannerMigrationFixture : SpannerFixtureBase, ICollectionFixture<SpannerMigrationFixture>
    {
        public SpannerMigrationFixture()
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

        private void CreateTables()
        {
            using var context = new TestMigrationDbContext(Database.DatabaseName);
            context.Database.Migrate();
        }

        private void ClearTables()
        {
            using (var con = GetConnection())
            {
                con.RunWithRetriableTransaction(tx =>
                {
                    var cmd = tx.CreateBatchDmlCommand();
                    foreach (var table in new string[]
                    {
                        "Products",
                        "Brands",
                        "Orders",
                        "OrderDetails"
                    })
                    {
                        cmd.Add($"DELETE FROM {table} WHERE TRUE");
                    }
                    cmd.ExecuteNonQuery();
                });
            }
        }
    }
}
