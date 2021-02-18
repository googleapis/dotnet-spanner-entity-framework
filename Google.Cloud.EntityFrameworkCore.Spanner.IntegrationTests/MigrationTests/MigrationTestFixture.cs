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
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.V1.Internal.Logging;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using System.Collections.Generic;
using System.Linq;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    /// <summary>
    /// DbContext for Migration tables.
    /// </summary>
    internal class TestMigrationDbContext : MigrationDbContext
    {
        public TestMigrationDbContext()
        {
        }

        private readonly DatabaseName _databaseName;

        internal TestMigrationDbContext(DatabaseName databaseName) => _databaseName = databaseName;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner($"Data Source={_databaseName};emulatordetection=EmulatorOrProduction");
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            // Override some settings if the tests are executed against the emulator, as the emulator does
            // not support all features of Spanner.
            if (SpannerFixtureBase.IsEmulator)
            {
                // Simulate a generated column when testing against the emulator.
                modelBuilder.Entity<Author>(entity =>
                {
                    entity.Property(e => e.FullName)
                        .HasValueGenerator<AuthorFullNameGenerator>()
                        .ValueGeneratedNever();
                });

                modelBuilder.Entity<AllColType>(entity =>
                {
                    entity.Ignore(e => e.ColDecimal);

                    // Configure the numeric columns for automatic conversion to/from FLOAT64 as the emulator does not support NUMERIC.
                    entity.Property(e => e.ColDecimal)
                        .HasConversion(
                            v => v.HasValue ? (double)v.Value.ToDecimal(LossOfPrecisionHandling.Truncate) : 0d,
                            v => new SpannerNumeric?(SpannerNumeric.FromDecimal((decimal)v, LossOfPrecisionHandling.Truncate))
                        )
                        .HasDefaultValue(new SpannerNumeric());
                    entity.Property(e => e.ColDecimalList)
                        .HasConversion(
                            v => v == null ? new List<double>() : v.Select(element => (double)element.ToDecimal(LossOfPrecisionHandling.Truncate)).ToList(),
                            v => v == null ? new List<SpannerNumeric>() : v.Select(element => SpannerNumeric.FromDecimal((decimal)element, LossOfPrecisionHandling.Truncate)).ToList()
                        );
                    entity.Property(e => e.ColDecimalArray)
                        .HasConversion(
                            v => v == null ? new double[0] : v.Select(element => (double)element.ToDecimal(LossOfPrecisionHandling.Truncate)).ToArray(),
                            v => v == null ? new SpannerNumeric[0] : v.Select(element => SpannerNumeric.FromDecimal((decimal)element, LossOfPrecisionHandling.Truncate)).ToArray()
                        );
                });
            }
        }
    }

    internal class AuthorFullNameGenerator : ValueGenerator<string>
    {
        public override bool GeneratesTemporaryValues => false;

        public override string Next(EntityEntry entry)
        {
            var author = entry.Entity as Author;
            return (author.FirstName ?? "") + " " + (author.LastName ?? "");
        }
    }

    public class MigrationTestFixture : SpannerFixtureBase
    {
        public MigrationTestFixture()
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
                foreach (var table in new string[]
                {
                        "OrderDetails",
                        "Orders",
                        "Products",
                        "Categories",
                        "AllColTypes",
                        "Articles",
                        "Authors"
                })
                {
                    cmd.Add($"DELETE FROM {table} WHERE TRUE");
                }
                return cmd.ExecuteNonQueryAsync();
            }).ResultWithUnwrappedExceptions();
        }

        /// <summary>
        /// Applies all Pending migrations.
        /// </summary>
        private void ApplyMigration()
        {
            using var context = new TestMigrationDbContext(Database.DatabaseName);
            context.Database.Migrate();
        }
    }
}
