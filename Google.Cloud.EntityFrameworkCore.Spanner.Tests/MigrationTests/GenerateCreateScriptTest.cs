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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests.Models;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests
{
    internal class MockMigrationSampleDbContext : SpannerMigrationSampleDbContext
    {
        private readonly string _connectionString;

        public MockMigrationSampleDbContext() : this("Data Source=projects/p1/instances/i1/databases/d1;")
        {
        }

        public MockMigrationSampleDbContext(string connectionString)
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
#pragma warning disable EF1001
                    .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
#pragma warning restore EF1001
                    .UseMutations(MutationUsage.Never)
                    .UseLazyLoadingProxies();
            }
        }
    }

    public class GenerateCreateScriptTest
    {
        [Fact]
        public void Generate_Create_Script()
        {
            using var db = new MockMigrationSampleDbContext();
            var generatedScript = db.Database.GenerateCreateScript();
            var script = @"CREATE TABLE `Singers` (
    `SingerId` INT64 NOT NULL,
    `FirstName` STRING(200),
    `LastName` STRING(200) NOT NULL,
    `FullName` STRING(400) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED,
    `BirthDate` DATE,
    `Picture` BYTES(MAX)
)PRIMARY KEY (`SingerId`)

CREATE TABLE `TableWithAllColumnTypes` (
    `ColSequence` INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE MySequence)),
    `ColInt64` INT64 NOT NULL,
    `ColFloat64` FLOAT64,
    `ColNumeric` NUMERIC,
    `ColBool` BOOL,
    `ColString` STRING(100),
    `ColStringMax` STRING(MAX),
    `ColChar` STRING(1),
    `ColBytes` BYTES(100),
    `ColBytesMax` BYTES(MAX),
    `ColDate` DATE,
    `ColTimestamp` TIMESTAMP,
    `ColCommitTS` TIMESTAMP OPTIONS (allow_commit_timestamp=true) ,
    `ColInt64Array` ARRAY<INT64>,
    `ColFloat64Array` ARRAY<FLOAT64>,
    `ColNumericArray` ARRAY<NUMERIC>,
    `ColBoolArray` ARRAY<BOOL>,
    `ColStringArray` ARRAY<STRING(100)>,
    `ColStringMaxArray` ARRAY<STRING(MAX)>,
    `ColBytesArray` ARRAY<BYTES(100)>,
    `ColBytesMaxArray` ARRAY<BYTES(MAX)>,
    `ColDateArray` ARRAY<DATE>,
    `ColTimestampArray` ARRAY<TIMESTAMP>,
    `ColGuid` STRING(36),
    `ColComputed` STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED
)PRIMARY KEY (`ColSequence`)

CREATE TABLE `Venues` (
    `Code` STRING(10) NOT NULL,
    `Name` STRING(100),
    `Active` BOOL NOT NULL,
    `Capacity` INT64,
    `Ratings` ARRAY<FLOAT64>
)PRIMARY KEY (`Code`)

CREATE TABLE `Albums` (
    `AlbumId` INT64 NOT NULL,
    `Title` STRING(100) NOT NULL,
    `ReleaseDate` DATE,
    `SingerId` INT64 NOT NULL,
    `MarketingBudget` INT64,
 CONSTRAINT `FK_Albums_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`),
)PRIMARY KEY (`AlbumId`)

CREATE TABLE `Concerts` (
    `VenueCode` STRING(10) NOT NULL,
    `StartTime` TIMESTAMP NOT NULL,
    `SingerId` INT64 NOT NULL,
    `Title` STRING(200),
 CONSTRAINT `FK_Concerts_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`),
 CONSTRAINT `FK_Concerts_Venues` FOREIGN KEY (`VenueCode`) REFERENCES `Venues` (`Code`),
)PRIMARY KEY (`VenueCode`, `StartTime`, `SingerId`)

CREATE TABLE `Tracks` (
    `AlbumId` INT64 NOT NULL,
    `TrackId` INT64 NOT NULL,
    `Title` STRING(200) NOT NULL,
    `Duration` NUMERIC,
    `LyricsLanguages` ARRAY<STRING(2)>,
    `Lyrics` ARRAY<STRING(MAX)>,
CONSTRAINT `Chk_Languages_Lyrics_Length_Equal` CHECK (ARRAY_LENGTH(LyricsLanguages) = ARRAY_LENGTH(Lyrics)),
)PRIMARY KEY (`AlbumId`, `TrackId`),
 INTERLEAVE IN PARENT `Albums` ON DELETE NO ACTION 


CREATE TABLE `Performances` (
    `VenueCode` STRING(10) NOT NULL,
    `SingerId` INT64 NOT NULL,
    `StartTime` TIMESTAMP NOT NULL,
    `ConcertStartTime` TIMESTAMP NOT NULL,
    `AlbumId` INT64 NOT NULL,
    `TrackId` INT64 NOT NULL,
    `Rating` FLOAT64,
 CONSTRAINT `FK_Performances_Concerts` FOREIGN KEY (`VenueCode`, `ConcertStartTime`, `SingerId`) REFERENCES `Concerts` (`VenueCode`, `StartTime`, `SingerId`),
 CONSTRAINT `FK_Performances_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`),
 CONSTRAINT `FK_Performances_Tracks` FOREIGN KEY (`AlbumId`, `TrackId`) REFERENCES `Tracks` (`AlbumId`, `TrackId`),
)PRIMARY KEY (`VenueCode`, `SingerId`, `StartTime`)

CREATE INDEX `AlbumsByAlbumTitle2` ON `Albums` (`Title`) STORING (`MarketingBudget`, `ReleaseDate`)

CREATE INDEX `Idx_Singers_FullName` ON `Singers` (`FullName`)

CREATE NULL_FILTERED INDEX `IDX_TableWithAllColumnTypes_ColDate_ColCommitTS` ON `TableWithAllColumnTypes` (`ColDate`, `ColCommitTS`)

CREATE UNIQUE INDEX `Idx_Tracks_AlbumId_Title` ON `Tracks` (`TrackId`, `Title`)

";
            Assert.Equal(script, generatedScript);
        }
    }
}
