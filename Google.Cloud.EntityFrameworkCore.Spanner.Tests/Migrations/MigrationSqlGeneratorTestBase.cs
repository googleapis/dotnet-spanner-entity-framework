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

using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests.Models;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Xunit;
using SpannerDate = Google.Cloud.EntityFrameworkCore.Spanner.Storage.SpannerDate;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Migrations
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public abstract class MigrationSqlGeneratorTestBase
    {
        protected static string EOL => Environment.NewLine;

        protected virtual string Sql { get; set; }

        public virtual void CreateTableOperation()
        {
            Generate(new CreateTableOperation
            {
                Name = "Albums",
                Columns =
                     {
                        new AddColumnOperation
                        {
                            Name = "AlbumId",
                            Table = "Albums",
                            ClrType = typeof(long),
                            IsNullable = false
                        },
                        new AddColumnOperation
                        {
                            Name = "Title",
                            Table = "Albums",
                            ClrType = typeof(string),
                            IsNullable = false,
                        },
                        new AddColumnOperation
                        {
                            Name = "ReleaseDate",
                            Table = "Albums",
                            ClrType = typeof(SpannerDate),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "SingerId",
                            Table = "Albums",
                            ClrType = typeof(long),
                            IsNullable = false
                        }
                     },
                PrimaryKey = new AddPrimaryKeyOperation { Columns = new[] { "AlbumId" } },
                CheckConstraints = { new AddCheckConstraintOperation { Name = "Chk_Title_Length_Equal", Sql = "CHARACTER_LENGTH(Title) > 0" } },
                ForeignKeys =
                     {
                        new AddForeignKeyOperation
                        {
                            Name = "FK_Albums_Singers",
                            Columns = new[] { "SingerId" },
                            PrincipalTable = "Singers",
                            PrincipalColumns = new[] { "SingerId" },
                        }
                     },
            });
        }

        public virtual void CreateTableWithAllColTypes()
        {
            Generate(new CreateTableOperation
            {
                Name = "AllColTypes",
                Columns =
                    {
                        new AddColumnOperation
                        {
                            Name = "Id",
                            Table = "AllColTypes",
                            ClrType = typeof(int),
                            IsNullable = false
                        },
                        new AddColumnOperation
                        {
                            Name = "ColShort",
                            Table = "AllColTypes",
                            ClrType = typeof(short),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColLong",
                            Table = "AllColTypes",
                            ClrType = typeof(long),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColByte",
                            Table = "AllColTypes",
                            ClrType = typeof(byte),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColSbyte",
                            Table = "AllColTypes",
                            ClrType = typeof(sbyte),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColULong",
                            Table = "AllColTypes",
                            ClrType = typeof(ulong),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColUShort",
                            Table = "AllColTypes",
                            ClrType = typeof(ushort),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDecimal",
                            Table = "AllColTypes",
                            ClrType = typeof(SpannerNumeric),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColUint",
                            Table = "AllColTypes",
                            ClrType = typeof(uint),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBool",
                            Table = "AllColTypes",
                            ClrType = typeof(bool),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDate",
                            Table = "AllColTypes",
                            ClrType = typeof(SpannerDate),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColTimestamp",
                            Table = "AllColTypes",
                            ClrType = typeof(DateTime),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColCommitTimestamp",
                            Table = "AllColTypes",
                            ClrType = typeof(DateTime),
                            IsNullable = true,
                            [SpannerAnnotationNames.UpdateCommitTimestamp] = SpannerUpdateCommitTimestamp.OnInsertAndUpdate
                        },
                        new AddColumnOperation
                        {
                            Name = "ColFloat",
                            Table = "AllColTypes",
                            ClrType = typeof(float),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDouble",
                            Table = "AllColTypes",
                            ClrType = typeof(double),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColString",
                            Table = "AllColTypes",
                            ClrType = typeof(string),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColGuid",
                            Table = "AllColTypes",
                            ClrType = typeof(Guid),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBytes",
                            Table = "AllColTypes",
                            ClrType = typeof(byte[]),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColJson",
                            Table = "AllColTypes",
                            ClrType = typeof(JsonDocument),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDecimalArray",
                            Table = "AllColTypes",
                            ClrType = typeof(SpannerNumeric[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDecimalList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<SpannerNumeric>),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColStringArray",
                            Table = "AllColTypes",
                            ClrType = typeof(string[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColStringList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<string>),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBoolArray",
                            Table = "AllColTypes",
                            ClrType = typeof(bool[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBoolList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<bool>),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDoubleArray",
                            Table = "AllColTypes",
                            ClrType = typeof(double[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDoubleList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<double>),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColLongArray",
                            Table = "AllColTypes",
                            ClrType = typeof(long[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColLongList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<long>),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDateArray",
                            Table = "AllColTypes",
                            ClrType = typeof(SpannerDate[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColDateList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<SpannerDate>),
                            IsNullable = true,
                        },
                        new AddColumnOperation
                        {
                            Name = "ColTimestampArray",
                            Table = "AllColTypes",
                            ClrType = typeof(DateTime[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColTimestampList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<DateTime>),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBytesArray",
                            Table = "AllColTypes",
                            ClrType = typeof(byte[][]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColBytesList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<byte[]>),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColJsonArray",
                            Table = "AllColTypes",
                            ClrType = typeof(JsonDocument[]),
                            IsNullable = true
                        },
                        new AddColumnOperation
                        {
                            Name = "ColJsonList",
                            Table = "AllColTypes",
                            ClrType = typeof(List<JsonDocument>),
                            IsNullable = true
                        },
                    },
                PrimaryKey = new AddPrimaryKeyOperation { Columns = new[] { "AlbumId" } },
            });
        }

        public virtual void CreateTableOperation_no_key()
            => Generate(new CreateTableOperation
            {
                Name = "Anonymous",
                Columns =
                    {
                        new AddColumnOperation
                        {
                            Name = "Value",
                            Table = "Anonymous",
                            ClrType = typeof(int),
                            IsNullable = false
                        }
                    }
            });

        public virtual void CreateIndexOperation()
            => Generate(
                modelBuilder => modelBuilder.Entity("Singer").Property<string>("FullName").IsRequired(),
                new CreateIndexOperation
                {
                    Name = "IX_Singer_FullName",
                    Table = "Singer",
                    Columns = new[] { "FullName" },
                });

        public virtual void CreateIndexOperation_is_null_filtered()
            => Generate(
                modelBuilder => modelBuilder.Entity("Singer").Property<string>("FullName"),
                new CreateIndexOperation
                {
                    Name = "IX_Singer_FullName",
                    Table = "Singer",
                    Columns = new[] { "FullName" },
                    [SpannerAnnotationNames.IsNullFilteredIndex] = true
                });

        public virtual void CreateIndexOperation_is_unique()
        {
            Generate(
                modelBuilder => modelBuilder.Entity("Singer").Property<string>("FullName"),
                new CreateIndexOperation
                {
                    Name = "IX_Singer_FullName",
                    Table = "Singer",
                    Columns = new[] { "FullName" },
                    IsUnique = true
                });
        }

        public virtual void CreateIndexOperation_storing()
            => Generate(
                modelBuilder => modelBuilder.Entity("Albums").Property<string>("AlbumTitle"),
                new CreateIndexOperation
                {
                    Name = "AlbumsByAlbumTitle2",
                    Table = "Albums",
                    Columns = new[] { "AlbumTitle" },
                    [SpannerAnnotationNames.Storing] = new string[] { "MarketingBudget" }
                });

        public virtual void CreateIndexOperation_storing_with_multiple_columns()
            => Generate(
                modelBuilder => modelBuilder.Entity("Albums").Property<string>("AlbumTitle"),
                new CreateIndexOperation
                {
                    Name = "AlbumsByAlbumTitle2",
                    Table = "Albums",
                    Columns = new[] { "AlbumTitle" },
                    [SpannerAnnotationNames.Storing] = new string[] { "MarketingBudget", "ReleaseDate" }
                });

        public virtual void AddColumOperation()
            => Generate(new AddColumnOperation
            {
                Table = "Singer",
                Name = "Name",
                ClrType = typeof(string),
                ColumnType = "STRING(30)",
                IsNullable = false
            });

        public virtual void AddColumnOperation_with_computedSql()
            => Generate(new AddColumnOperation
            {
                Table = "Singer",
                Name = "FullName",
                ClrType = typeof(string),
                ComputedColumnSql = "(COALESCE(FirstName || ' ', '') || LastName) STORED"
            });

        public virtual void AddColumnOperation_with_update_commit_timestamp()
            => Generate(new AddColumnOperation
            {
                Table = "Album",
                Name = "CreatedDate",
                ClrType = typeof(DateTime),
                [SpannerAnnotationNames.UpdateCommitTimestamp] = SpannerUpdateCommitTimestamp.OnInsertAndUpdate
            });

        public virtual void AddColumnOperation_without_column_type()
            => Generate(new AddColumnOperation
            {
                Table = "Singer",
                Name = "FullName",
                ClrType = typeof(string)
            });

        public virtual void AddColumnOperation_with_column_type()
            => Generate(new AddColumnOperation
            {
                Table = "Singer",
                Name = "FullName",
                ClrType = typeof(string),
                ColumnType = "ARRAY<STRING(200)>"
            });

        public virtual void AddColumnOperation_with_maxLength()
            => Generate(
                modelBuilder => modelBuilder.Entity("Singer").Property<string>("FullName").HasMaxLength(30),
                new AddColumnOperation
                {
                    Table = "Singer",
                    Name = "FullName",
                    ClrType = typeof(string),
                    MaxLength = 30,
                    IsNullable = true
                });

        public virtual void AddColumnOperation_with_maxLength_overridden()
            => Generate(
                modelBuilder => modelBuilder.Entity("Singer").Property<string>("FullName").HasMaxLength(30),
                new AddColumnOperation
                {
                    Table = "Singer",
                    Name = "FullName",
                    ClrType = typeof(string),
                    MaxLength = 32,
                    IsNullable = true
                });

        public virtual void AddColumnOperation_with_maxLength_no_model()
            => Generate(
                new AddColumnOperation
                {
                    Table = "Singer",
                    Name = "FullName",
                    ClrType = typeof(string),
                    MaxLength = 30,
                    IsNullable = true
                });

        public virtual void AddColumnOperation_with_maxLength_on_derived()
            => Generate(
                modelBuilder =>
                {
                    modelBuilder.Entity("Singer");
                    modelBuilder.Entity(
                        "SpecialSinger", b =>
                        {
                            b.HasBaseType("Singer");
                            b.Property<string>("Name").HasMaxLength(30);
                        });

                    modelBuilder.Entity("MoreSpecialSinger").HasBaseType("SpecialSinger");
                }, new AddColumnOperation
                {
                    Table = "Singer",
                    Name = "Name",
                    ClrType = typeof(string),
                    MaxLength = 30,
                    IsNullable = true
                });

        public virtual void AddColumnOperation_with_shared_column()
            => Generate(
                modelBuilder =>
                {
                    modelBuilder.Entity<VersionedEntity>();
                    modelBuilder.Entity<Derived1>();
                    modelBuilder.Entity<Derived2>();
                },
                new AddColumnOperation
                {
                    Table = "VersionedEntity",
                    Name = "Version",
                    ClrType = typeof(long),
                    IsNullable = true
                });

        public virtual void AddForeignKeyOperation_with_name()
            => Generate(
                new AddForeignKeyOperation
                {
                    Table = "Album",
                    Name = "FK_Album_Singer",
                    Columns = new[] { "SingerId" },
                    PrincipalTable = "Singer",
                    PrincipalColumns = new[] { "SingerId" },
                    OnDelete = ReferentialAction.Cascade
                });

        public virtual void AddForeignKeyOperation_with_multiple_column()
            => Generate(new AddForeignKeyOperation
            {
                Table = "Performances",
                Name = "FK_Performances_Concerts",
                Columns = new[] { "VenueCode", "ConcertStartTime", "SingerId" },
                PrincipalTable = "Concerts",
                PrincipalColumns = new[] { "VenueCode", "StartTime", "SingerId" }
            });

        public virtual void CreateCheckConstraintOperation_with_name()
            => Generate(
                new AddCheckConstraintOperation
                {
                    Table = "Singers",
                    Name = "Chk_Title_Length_Equal",
                    Sql = "CHARACTER_LENGTH(Title) > 0"
                });

        public virtual void DropColumnOperation()
            => Generate(
                new DropColumnOperation
                {
                    Table = "Singer",
                    Name = "FullName"
                });

        public virtual void DropForeignKeyOperation()
            => Generate(
                new DropForeignKeyOperation
                {
                    Table = "Album",
                    Name = "FK_Album_Singers"
                });

        public virtual void DropIndexOperation()
            => Generate(new DropIndexOperation
            {
                Name = "IX_Singer_FullName",
                Table = "Singer",
            });

        public virtual void DropTableOperation()
            => Generate(new DropTableOperation { Name = "Singer" });

        public virtual void DropCheckConstraintOperation()
            => Generate(
                new DropCheckConstraintOperation
                {
                    Table = "Singer",
                    Name = "CK_Singer_FullName"
                });

        public virtual void RenameColumnOperation()
            => Generate(
                new RenameColumnOperation
                {
                    Table = "Singer",
                    Name = "Name",
                    NewName = "FullName"
                });

        public virtual void InsertDataOperation()
            => Generate(
                new InsertDataOperation
                {
                    Table = "Singers",
                    Columns = new[] { "SingerId", "FirstName", "LastName" },
                    ColumnTypes = new []{ "INT64", "STRING", "STRING" },
                    
                    Values = new object[,] {
                        { 1, "Marc", "Richards" }, { 2, "Catalina", "Smith" }, { 3, "Alice", "Trentor" }, { 4, "Lea", "Martin" }
                    }
                });

        public virtual void DeleteDataOperation_simple_key()
            => Generate(
                new DeleteDataOperation
                {
                    Table = "Singers",
                    KeyColumns = new[] { "SingerId" },
                    KeyValues = new object[,] { { 1 }, { 3 } }
                });

        public virtual void DeleteDataOperation_composite_key()
            => Generate(
                new DeleteDataOperation
                {
                    Table = "Singers",
                    KeyColumns = new[] { "FirstName", "LastName" },
                    KeyValues = new object[,] { { "Dorothy", null }, { "Curt", "Lee" } }
                });

        public virtual void UpdateDataOperation_simple_key()
            => Generate(
                new UpdateDataOperation
                {
                    Table = "Singers",
                    KeyColumns = new[] { "SingerId" },
                    KeyValues = new object[,] { { 1 }, { 4 } },
                    Columns = new[] { "FirstName" },
                    Values = new object[,] { { "Christopher" }, { "Lisa" } }
                });

        public virtual void UpdateDataOperation_composite_key()
            => Generate(
                new UpdateDataOperation
                {
                    Table = "Albums",
                    KeyColumns = new[] { "SingerId", "AlbumId" },
                    KeyValues = new object[,] { { 1, 1 }, { 1, 2 } },
                    Columns = new[] { "Title" },
                    Values = new object[,] { { "Total Junk" }, { "Terrified" } }
                });

        public virtual void UpdateDataOperation_multiple_columns()
            => Generate(
                new UpdateDataOperation
                {
                    Table = "Singers",
                    KeyColumns = new[] { "SingerId" },
                    KeyValues = new object[,] { { 1 }, { 4 } },
                    Columns = new[] { "FirstName", "LastName" },
                    Values = new object[,] { { "Gregory", "Davis" }, { "Katherine", "Palmer" } }
                });

        public virtual void AlterColumnOperation()
            => Generate(new AlterColumnOperation
            {
                Table = "Singers",
                Name = "CharColumn",
                ClrType = typeof(string),
                IsNullable = true
            });

        public virtual void AlterColumnOperation_Add_Commit_Timestamp()
            => Generate(new AlterColumnOperation
            {
                Table = "Singers",
                Name = "ColCommitTimestamp",
                ClrType = typeof(DateTime),
                ["UpdateCommitTimestamp"] = SpannerUpdateCommitTimestamp.OnInsertAndUpdate
            });

        public virtual void AlterColumnOperation_Remove_Commit_Timestamp()
            => Generate(new AlterColumnOperation
            {
                Table = "Singers",
                Name = "ColCommitTimestamp",
                ClrType = typeof(DateTime),
                ["UpdateCommitTimestamp"] = SpannerUpdateCommitTimestamp.Never
            });

        public virtual void AlterColumnOperation_Make_type_not_null()
            => Generate(new AlterColumnOperation
            {
                Table = "Singers",
                Name = "ColLong",
                ClrType = typeof(long),
            });

        public virtual void AlterColumnOperation_Make_type_nullable()
            => Generate(new AlterColumnOperation
            {
                Table = "Singers",
                Name = "ColLong",
                ClrType = typeof(long),
                IsNullable = true
            });

        private class VersionedEntity
        {
            public long Version { get; set; }
        }

        private class Derived1 : VersionedEntity
        {
            public string Foo { get; set; }
        }

        private class Derived2 : VersionedEntity
        {
            public string Foo { get; set; }
        }
        protected TestHelpers TestHelpers { get; }

        protected MigrationSqlGeneratorTestBase(TestHelpers testHelpers)
        {
            TestHelpers = testHelpers;
        }

        protected virtual void Generate(params MigrationOperation[] operation)
            => Generate(_ => { }, operation);

        protected virtual void Generate(Action<ModelBuilder> buildAction, params MigrationOperation[] operation)
        {
            var modelBuilder = TestHelpers.CreateConventionBuilder();
            modelBuilder.Entity<Singers>();
            modelBuilder.Entity<Albums>();
            modelBuilder.Model.RemoveAnnotation(CoreAnnotationNames.ProductVersion);
            
            buildAction(modelBuilder);

            var finalizedModel = modelBuilder.Model.FinalizeModel();
            var services = TestHelpers.CreateContextServices();
            services.GetRequiredService<IModelRuntimeInitializer>().Initialize(finalizedModel);
            
            var batch = services
                .GetRequiredService<IMigrationsSqlGenerator>()
                .Generate(operation, finalizedModel);

            Sql = string.Join(EOL, batch.Select(b => b.CommandText));
        }

        protected void AssertSql(string expected)
            => Assert.Equal(expected, Sql, ignoreLineEndingDifferences: true);
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
