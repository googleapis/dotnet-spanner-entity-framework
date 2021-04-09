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

using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using System;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Migrations
{
    public class SpannerMigrationSqlGeneratorTest : MigrationSqlGeneratorTestBase
    {
        [Fact]
        public override void CreateTableOperation()
        {
            base.CreateTableOperation();
            AssertSql(
                @"CREATE TABLE `Albums` (
    `AlbumId` INT64 NOT NULL,
    `Title` STRING(MAX) NOT NULL,
    `ReleaseDate` DATE,
    `SingerId` INT64 NOT NULL,
 CONSTRAINT `FK_Albums_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`),
CONSTRAINT `Chk_Title_Length_Equal` CHECK (CHARACTER_LENGTH(Title) > 0),
)PRIMARY KEY (`AlbumId`)");
        }

        [Fact]
        public override void CreateTableWithAllColTypes()
        {
            base.CreateTableWithAllColTypes();
            AssertSql(
                @"CREATE TABLE `AllColTypes` (
    `Id` INT64 NOT NULL,
    `ColShort` INT64,
    `ColLong` INT64,
    `ColByte` INT64,
    `ColSbyte` INT64,
    `ColULong` INT64,
    `ColUShort` INT64,
    `ColDecimal` NUMERIC,
    `ColUint` INT64,
    `ColBool` BOOL,
    `ColDate` DATE,
    `ColTimestamp` TIMESTAMP,
    `ColCommitTimestamp` TIMESTAMP OPTIONS (allow_commit_timestamp=true) ,
    `ColFloat` FLOAT64,
    `ColDouble` FLOAT64,
    `ColString` STRING(MAX),
    `ColGuid` STRING(36),
    `ColBytes` BYTES(MAX),
    `ColDecimalArray` ARRAY<NUMERIC>,
    `ColDecimalList` ARRAY<NUMERIC>,
    `ColStringArray` ARRAY<STRING(MAX)>,
    `ColStringList` ARRAY<STRING(MAX)>,
    `ColBoolArray` ARRAY<BOOL>,
    `ColBoolList` ARRAY<BOOL>,
    `ColDoubleArray` ARRAY<FLOAT64>,
    `ColDoubleList` ARRAY<FLOAT64>,
    `ColLongArray` ARRAY<INT64>,
    `ColLongList` ARRAY<INT64>,
    `ColDateArray` ARRAY<DATE>,
    `ColDateList` ARRAY<DATE>,
    `ColTimestampArray` ARRAY<TIMESTAMP>,
    `ColTimestampList` ARRAY<TIMESTAMP>,
    `ColBytesArray` ARRAY<BYTES(MAX)>,
    `ColBytesList` ARRAY<BYTES(MAX)>
)PRIMARY KEY (`AlbumId`)");
        }

        [Fact]
        public override void CreateTableOperation_no_key()
        {
            base.CreateTableOperation_no_key();
            AssertSql(
                @"CREATE TABLE `Anonymous` (
    `Value` INT64 NOT NULL
)");
        }

        [Fact]
        public override void CreateIndexOperation()
        {
            base.CreateIndexOperation();
            AssertSql(@"CREATE INDEX `IX_Singer_FullName` ON `Singer` (`FullName`)");
        }

        [Fact]
        public override void CreateIndexOperation_is_null_filtered()
        {
            base.CreateIndexOperation_is_null_filtered();
            AssertSql(@"CREATE NULL_FILTERED INDEX `IX_Singer_FullName` ON `Singer` (`FullName`)");
        }

        [Fact]
        public override void CreateIndexOperation_is_unique()
        {
            base.CreateIndexOperation_is_unique();
            AssertSql(@"CREATE UNIQUE INDEX `IX_Singer_FullName` ON `Singer` (`FullName`)");
        }

        [Fact]
        public override void CreateIndexOperation_storing()
        {
            base.CreateIndexOperation_storing();
            AssertSql(@"CREATE INDEX `AlbumsByAlbumTitle2` ON `Albums` (`AlbumTitle`) STORING (`MarketingBudget`)");
        }

        [Fact]
        public override void CreateIndexOperation_storing_with_multiple_columns()
        {
            base.CreateIndexOperation_storing_with_multiple_columns();
            AssertSql(@"CREATE INDEX `AlbumsByAlbumTitle2` ON `Albums` (`AlbumTitle`) STORING (`MarketingBudget`, `ReleaseDate`)");
        }

        [Fact]
        public override void AddColumOperation()
        {
            base.AddColumOperation();
            AssertSql(@"ALTER TABLE `Singer` ADD `Name` STRING(30) NOT NULL
");
        }

        [Fact]
        public override void AddColumnOperation_with_computedSql()
        {
            base.AddColumnOperation_with_computedSql();
            AssertSql(@"ALTER TABLE `Singer` ADD `FullName` STRING(MAX) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED
");
        }

        [Fact]
        public override void AddColumnOperation_with_update_commit_timestamp()
        {
            base.AddColumnOperation_with_update_commit_timestamp();
            AssertSql(@"ALTER TABLE `Album` ADD `CreatedDate` TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true) 
");
        }

        [Fact]
        public override void AddColumnOperation_without_column_type()
        {
            base.AddColumnOperation_without_column_type();
            AssertSql(@"ALTER TABLE `Singer` ADD `FullName` STRING(MAX) NOT NULL
");
        }

        [Fact]
        public override void AddColumnOperation_with_column_type()
        {
            base.AddColumnOperation_with_column_type();
            AssertSql(@"ALTER TABLE `Singer` ADD `FullName` ARRAY<STRING(200)> NOT NULL
");
        }

        [Fact]
        public override void AddColumnOperation_with_maxLength()
        {
            base.AddColumnOperation_with_maxLength();
            AssertSql(@"ALTER TABLE `Singer` ADD `FullName` STRING(30)
");
        }

        [Fact]
        public override void AddColumnOperation_with_maxLength_no_model()
        {
            base.AddColumnOperation_with_maxLength_no_model();
            AssertSql(@"ALTER TABLE `Singer` ADD `FullName` STRING(30)
");
        }

        [Fact]
        public override void AddColumnOperation_with_maxLength_overridden()
        {
            base.AddColumnOperation_with_maxLength_overridden();
            AssertSql(@"ALTER TABLE `Singer` ADD `FullName` STRING(32)
");
        }

        [Fact]
        public override void AddColumnOperation_with_maxLength_on_derived()
        {
            base.AddColumnOperation_with_maxLength_on_derived();
            AssertSql(@"ALTER TABLE `Singer` ADD `Name` STRING(30)
");
        }

        [Fact]
        public override void AddColumnOperation_with_shared_column()
        {
            base.AddColumnOperation_with_shared_column();
            AssertSql(@"ALTER TABLE `VersionedEntity` ADD `Version` INT64
");
        }

        [Fact]
        public virtual void AddColumnOperation_with_defaultValue()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new AddColumnOperation
                {
                    Table = "Album",
                    Name = "CreatedDate",
                    ClrType = typeof(DateTime),
                    ColumnType = "TIMESTAMP",
                    IsNullable = false,
                    DefaultValue = DateTime.UtcNow
                }));
        }

        [Fact]
        public override void AddForeignKeyOperation_with_name()
        {
            base.AddForeignKeyOperation_with_name();
            AssertSql(@"ALTER TABLE `Album` ADD  CONSTRAINT `FK_Album_Singer` FOREIGN KEY (`SingerId`) REFERENCES `Singer` (`SingerId`)
");
        }

        [Fact]
        public override void AddForeignKeyOperation_with_multiple_column()
        {
            base.AddForeignKeyOperation_with_multiple_column();
            AssertSql(@"ALTER TABLE `Performances` ADD  CONSTRAINT `FK_Performances_Concerts` FOREIGN KEY (`VenueCode`, `ConcertStartTime`, `SingerId`) REFERENCES `Concerts` (`VenueCode`, `StartTime`, `SingerId`)
");
        }

        [Fact]
        public void AlterSequenceOperation_with_minValue_and_maxValue()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
               new AlterSequenceOperation
               {
                   Name = "SpannerkHiLoSequence",
                   IncrementBy = 1,
                   MinValue = 2,
                   MaxValue = 816,
                   IsCyclic = true
               }));
        }

        [Fact]
        public void CreateDatabaseOperation()
        {
            Generate(new SpannerCreateDatabaseOperation { Name = "Northwind" });
            AssertSql(@"CREATE DATABASE `Northwind`");
        }

        [Fact]
        public virtual void CreateSequenceOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new CreateSequenceOperation
                {
                    Name = "SpannerkHiLoSequence",
                    StartValue = 3,
                    IncrementBy = 1,
                    ClrType = typeof(long),
                    IsCyclic = true
                }));
        }

        [Fact]
        public override void CreateCheckConstraintOperation_with_name()
        {
            base.CreateCheckConstraintOperation_with_name();
            AssertSql(@"ALTER TABLE `Singers` ADD CONSTRAINT `Chk_Title_Length_Equal` CHECK (CHARACTER_LENGTH(Title) > 0)
");
        }

        [Fact]
        public override void DropColumnOperation()
        {
            base.DropColumnOperation();
            AssertSql(@"ALTER TABLE `Singer` DROP COLUMN `FullName`
");
        }

        [Fact]
        public override void DropForeignKeyOperation()
        {
            base.DropForeignKeyOperation();
            AssertSql(@"ALTER TABLE `Album` DROP CONSTRAINT `FK_Album_Singers`
");
        }

        [Fact]
        public override void DropIndexOperation()
        {
            base.DropIndexOperation();
            AssertSql(@" DROP INDEX `IX_Singer_FullName`");
        }

        [Fact]
        public override void DropTableOperation()
        {
            base.DropTableOperation();
            AssertSql(@"DROP TABLE `Singer`
");
        }

        [Fact]
        public override void DropCheckConstraintOperation()
        {
            base.DropCheckConstraintOperation();
            AssertSql(@"ALTER TABLE `Singer` DROP CONSTRAINT `CK_Singer_FullName`
");
        }

        [Fact]
        public void DropDatabaseOperation()
        {
            Generate(new SpannerDropDatabaseOperation { Name = "Northwind" });
            AssertSql(@"DROP DATABASE `Northwind`");
        }

        [Fact]
        public virtual void DropSequenceOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new DropSequenceOperation
                {
                    Name = "SpannerkHiLoSequence"
                }));
        }

        [Fact]
        public virtual void RenameSequenceOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new RenameSequenceOperation
                {
                    Name = "SpannerkHiLoSequence",
                    NewName = "SpannerkHiLoSequenceUpdated"
                }));
        }

        [Fact]
        public virtual void RestartSequenceOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new RestartSequenceOperation
                {
                    Name = "SpannerkHiLoSequence",
                    StartValue = 1
                }));
        }

        [Fact]
        public override void RenameColumnOperation()
        {
            Assert.Throws<NotSupportedException>(() => base.RenameColumnOperation());
        }

        [Fact]
        public virtual void AlterDatabaseOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new AlterDatabaseOperation
                {
                    ["TestAnnotation"] = "Value"
                }));
        }

        [Fact]
        public virtual void RenameIndexOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new RenameIndexOperation
                {
                    Table = "Singer",
                    Name = "IX_Singer_Name",
                    NewName = "IX_Singer_FullName"
                }));
        }

        [Fact]
        public virtual void RenameTableOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new RenameTableOperation
                {
                    Name = "People",
                    NewName = "Person"
                }));
        }

        [Fact]
        public virtual void AddUniqueConstraintOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new AddUniqueConstraintOperation
                {
                    Table = "Singer",
                    Name = "Unique_Name",
                    Columns = new[] { "FirstName", "LastName" }
                }));
        }

        [Fact]
        public virtual void DropUniqueConstraintOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new DropUniqueConstraintOperation
                {
                    Table = "Singer",
                    Name = "Unique_Name",
                }));
        }

        [Fact]
        public virtual void CreateSchemaOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new EnsureSchemaOperation { Name = "my" }));
        }

        [Fact]
        public virtual void DropSchemaOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new DropSchemaOperation
                {
                    Name = "dbo"
                }));
        }

        [Fact]
        public override void InsertDataOperation()
        {
            base.InsertDataOperation();
            AssertSql(@"INSERT INTO `Singer` (`SingerId`, `FirstName`, `LastName`)
VALUES (1, 'Marc', 'Richards'),
(2, 'Catalina', 'Smith'),
(3, 'Alice', 'Trentor'),
(4, 'Lea', 'Martin');
");
        }

        [Fact]
        public override void DeleteDataOperation_simple_key()
        {
            base.DeleteDataOperation_simple_key();
            AssertSql(@"DELETE FROM `Singer`
WHERE `SingerId` = 1;
DELETE FROM `Singer`
WHERE `SingerId` = 3;
");
        }

        [Fact]
        public override void DeleteDataOperation_composite_key()
        {
            base.DeleteDataOperation_composite_key();
            AssertSql(@"DELETE FROM `Singer`
WHERE `FirstName` = 'Dorothy' AND `LastName` IS NULL;
DELETE FROM `Singer`
WHERE `FirstName` = 'Curt' AND `LastName` = 'Lee';
");
        }

        [Fact]
        public override void UpdateDataOperation_simple_key()
        {
            base.UpdateDataOperation_simple_key();
            AssertSql(@"UPDATE `Singer` SET `FirstName` = 'Christopher'
WHERE `SingerId` = 1;
UPDATE `Singer` SET `FirstName` = 'Lisa'
WHERE `SingerId` = 4;
");
        }

        [Fact]
        public override void UpdateDataOperation_composite_key()
        {
            base.UpdateDataOperation_composite_key();
            AssertSql(@"UPDATE `Album` SET `Title` = 'Total Junk'
WHERE `SingerId` = 1 AND `AlbumId` = 1;
UPDATE `Album` SET `Title` = 'Terrified'
WHERE `SingerId` = 1 AND `AlbumId` = 2;
");
        }

        [Fact]
        public override void UpdateDataOperation_multiple_columns()
        {
            base.UpdateDataOperation_multiple_columns();
            AssertSql(@"UPDATE `Singer` SET `FirstName` = 'Gregory', `LastName` = 'Davis'
WHERE `SingerId` = 1;
UPDATE `Singer` SET `FirstName` = 'Katherine', `LastName` = 'Palmer'
WHERE `SingerId` = 4;
");
        }

        [Fact]
        public override void AlterColumnOperation()
        {
            base.AlterColumnOperation();
            AssertSql(@"ALTER TABLE `Singers` ALTER COLUMN `CharColumn` STRING(MAX)");
        }

        [Fact]
        public override void AlterColumnOperation_Add_Commit_Timestamp()
        {
            base.AlterColumnOperation_Add_Commit_Timestamp();
            AssertSql(@"ALTER TABLE `Singers` ALTER COLUMN `ColCommitTimestamp` SET OPTIONS (allow_commit_timestamp=true) ");
        }

        [Fact]
        public override void AlterColumnOperation_Remove_Commit_Timestamp()
        {
            base.AlterColumnOperation_Remove_Commit_Timestamp();
            AssertSql(@"ALTER TABLE `Singers` ALTER COLUMN `ColCommitTimestamp` SET OPTIONS (allow_commit_timestamp=null) ");
        }

        [Fact]
        public override void AlterColumnOperation_Make_type_not_null()
        {
            base.AlterColumnOperation_Make_type_not_null();
            AssertSql(@"ALTER TABLE `Singers` ALTER COLUMN `ColLong` INT64 NOT NULL");
        }

        [Fact]
        public override void AlterColumnOperation_Make_type_nullable()
        {
            base.AlterColumnOperation_Make_type_nullable();
            AssertSql(@"ALTER TABLE `Singers` ALTER COLUMN `ColLong` INT64");
        }

        [Fact]
        public virtual void AlterColumnOperation_set_default_value()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new AlterColumnOperation
                {
                    Table = "Singers",
                    Name = "Location",
                    ClrType = typeof(string),
                    DefaultValue = "London"
                }));
        }

        [Fact]
        public virtual void AddPrimaryKeyOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
               new AddPrimaryKeyOperation
               {
                   Table = "Singer",
                   Columns = new[] { "SingerId" },
                   Name = "PK_Singer"
               }));
        }

        [Fact]
        public virtual void DropPrimaryKeyOperation()
        {
            Assert.Throws<NotSupportedException>(() => Generate(
                new DropPrimaryKeyOperation
                {
                    Table = "Singer",
                    Name = "PK_Singer"
                }));
        }

        public SpannerMigrationSqlGeneratorTest()
            : base(SpannerTestHelpers.Instance)
        {
        }
    }
}
