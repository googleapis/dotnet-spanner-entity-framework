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

        public SpannerMigrationSqlGeneratorTest()
            : base(SpannerTestHelpers.Instance)
        {
        }
    }
}
