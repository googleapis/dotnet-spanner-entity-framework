﻿// Copyright 2021 Google LLC
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
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Migrations
{
    public class SpannerMigrationSqlGeneratorTest : MigrationSqlGeneratorTestBase
    {
        public override void CreateTableOperation()
        {
            base.CreateTableOperation();
            AssertSql(
                @"CREATE TABLE Albums (
    AlbumId INT64 NOT NULL,
    Title STRING(MAX) NOT NULL,
    ReleaseDate DATE,
    SingerId INT64 NOT NULL,
 CONSTRAINT FK_Albums_Singers FOREIGN KEY (SingerId) REFERENCES Singers (SingerId),
CONSTRAINT Chk_Title_Length_Equal CHECK (CHARACTER_LENGTH(Title) > 0),
)PRIMARY KEY (AlbumId)");
        }
        public override void CreateTableWithAllColTypes()
        {
            base.CreateTableWithAllColTypes();
            AssertSql(
                @"CREATE TABLE AllColTypes (
    Id INT64 NOT NULL,
    ColShort INT64,
    ColLong INT64,
    ColByte INT64,
    ColSbyte INT64,
    ColULong INT64,
    ColUShort INT64,
    ColDecimal NUMERIC,
    ColUint INT64,
    ColBool BOOL,
    ColDate DATE,
    ColTimestamp TIMESTAMP,
    ColCommitTimestamp TIMESTAMP OPTIONS (allow_commit_timestamp=true) ,
    ColFloat FLOAT64,
    ColDouble FLOAT64,
    ColString STRING(MAX),
    ColGuid STRING(MAX),
    ColBytes BYTES(MAX),
    ColDecimalArray ARRAY<NUMERIC>,
    ColDecimalList ARRAY<NUMERIC>,
    ColStringArray ARRAY<STRING(MAX)>,
    ColStringList ARRAY<STRING(MAX)>,
    ColBoolArray ARRAY<BOOL>,
    ColBoolList ARRAY<BOOL>,
    ColDoubleArray ARRAY<FLOAT64>,
    ColDoubleList ARRAY<FLOAT64>,
    ColLongArray ARRAY<INT64>,
    ColLongList ARRAY<INT64>,
    ColDateArray ARRAY<DATE>,
    ColDateList ARRAY<DATE>,
    ColTimestampArray ARRAY<TIMESTAMP>,
    ColTimestampList ARRAY<TIMESTAMP>,
    ColBytesArray ARRAY<BYTES(MAX)>,
    ColBytesList ARRAY<BYTES(MAX)>
)PRIMARY KEY (AlbumId)");
        }

        public override void CreateTableOperation_no_key()
        {
            base.CreateTableOperation_no_key();
            AssertSql(
                @"CREATE TABLE Anonymous (
    Value INT64 NOT NULL
)");
        }

        public override void CreateIndexOperation()
        {
            base.CreateIndexOperation();
            AssertSql(@"CREATE INDEX IX_Singer_FullName ON Singer (FullName)");
        }

        public override void CreateIndexOperation_is_null_filtered()
        {
            base.CreateIndexOperation_is_null_filtered();
            AssertSql(@"CREATE NULL_FILTERED INDEX IX_Singer_FullName ON Singer (FullName)");
        }

        public override void CreateIndexOperation_is_unique()
        {
            base.CreateIndexOperation_is_unique();
            AssertSql(@"CREATE UNIQUE INDEX IX_Singer_FullName ON Singer (FullName)");
        }

        public override void AddColumOperation()
        {
            base.AddColumOperation();
            AssertSql(@"ALTER TABLE Singer ADD Name STRING(30) NOT NULL
");
        }

        public override void AddColumnOperation_with_computedSql()
        {
            base.AddColumnOperation_with_computedSql();
            AssertSql(@"ALTER TABLE Singer ADD FullName STRING(MAX) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED
");
        }

        public override void AddColumnOperation_with_update_commit_timestamp()
        {
            base.AddColumnOperation_with_update_commit_timestamp();
            AssertSql(@"ALTER TABLE Album ADD CreatedDate STRING(MAX) NOT NULL OPTIONS (allow_commit_timestamp=true) 
");
        }

        public override void AddColumnOperation_without_column_type()
        {
            base.AddColumnOperation_without_column_type();
            AssertSql(@"ALTER TABLE Singer ADD FullName STRING(MAX) NOT NULL
");
        }

        public override void AddColumnOperation_with_column_type()
        {
            base.AddColumnOperation_with_column_type();
            AssertSql(@"ALTER TABLE Singer ADD FullName ARRAY<STRING(200)> NOT NULL
");
        }

        public override void AddColumnOperation_with_maxLength()
        {
            base.AddColumnOperation_with_maxLength();
            AssertSql(@"ALTER TABLE Singer ADD FullName STRING(30)
");
        }

        public override void AddColumnOperation_with_maxLength_no_model()
        {
            base.AddColumnOperation_with_maxLength_no_model();
            AssertSql(@"ALTER TABLE Singer ADD FullName STRING(30)
");
        }

        public override void AddColumnOperation_with_maxLength_overridden()
        {
            base.AddColumnOperation_with_maxLength_overridden();
            AssertSql(@"ALTER TABLE Singer ADD FullName STRING(32)
");
        }

        public override void AddColumnOperation_with_maxLength_on_derived()
        {
            base.AddColumnOperation_with_maxLength_on_derived();
            AssertSql(@"ALTER TABLE Singer ADD Name STRING(30)
");
        }

        public override void AddColumnOperation_with_shared_column()
        {
            base.AddColumnOperation_with_shared_column();
            AssertSql(@"ALTER TABLE VersionedEntity ADD Version INT64
");
        }

        public override void AddForeignKeyOperation_with_name()
        {
            base.AddForeignKeyOperation_with_name();
            AssertSql(@"ALTER TABLE Album ADD  CONSTRAINT FK_Album_Singer FOREIGN KEY (SingerId) REFERENCES Singer (SingerId),

");
        }

        [Fact]
        public void CreateDatabaseOperation()
        {
            Generate(new SpannerCreateDatabaseOperation { Name = "Northwind" });
            AssertSql(@"CREATE DATABASE Northwind");
        }

        public override void DropColumnOperation()
        {
            base.DropColumnOperation();
            AssertSql(@"ALTER TABLE Singer DROP COLUMN FullName
");
        }

        public override void DropForeignKeyOperation()
        {
            base.DropForeignKeyOperation();
            AssertSql(@"ALTER TABLE Album DROP CONSTRAINT FK_Album_Singers
");
        }

        public override void DropIndexOperation()
        {
            base.DropIndexOperation();
            var test = Sql;
            AssertSql(@" DROP INDEX IX_Singer_FullName");
        }

        public override void DropTableOperation()
        {
            base.DropTableOperation();
            AssertSql(@"DROP TABLE Singer
");
        }

        public override void DropCheckConstraintOperation()
        {
            base.DropCheckConstraintOperation();
            AssertSql(@"ALTER TABLE Singer DROP CONSTRAINT CK_Singer_FullName
");
        }

        [Fact]
        public void DropDatabaseOperation()
        {
            Generate(new SpannerDropDatabaseOperation { Name = "Northwind" });
            AssertSql(@"DROP DATABASE Northwind");
        }

        public SpannerMigrationSqlGeneratorTest()
            : base(SpannerTestHelpers.Instance)
        {
        }
    }
}