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

using System;
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

    internal class MockMigrationAutoIncrementSampleDbContext : MockMigrationSampleDbContext
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.UseAutoIncrement();
        }
    }

    internal class MockMigrationDisableIdentityColumnsSampleDbContext : MockMigrationSampleDbContext
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.DisableIdentityColumns();
        }
    }

    public class GenerateCreateScriptTest
    {
        [Fact]
        public void Generate_Create_Script()
        {
            using var db = new MockMigrationSampleDbContext();
            var generatedScript = db.Database.GenerateCreateScript();
            var script = $"CREATE TABLE `Singers` ({Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `FirstName` STRING(200),{Environment.NewLine}" +
            $"    `LastName` STRING(200) NOT NULL,{Environment.NewLine}" +
            $"    `FullName` STRING(400) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED,{Environment.NewLine}" +
            $"    `BirthDate` DATE,{Environment.NewLine}" +
            $"    `Picture` BYTES(MAX){Environment.NewLine}" +
            $")PRIMARY KEY (`SingerId`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `TableWithAllColumnTypes` ({Environment.NewLine}" +
            $"    `ColSequence` INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE MySequence)),{Environment.NewLine}" +
            $"    `ColInt64` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `ColFloat64` FLOAT64,{Environment.NewLine}" +
            $"    `ColNumeric` NUMERIC,{Environment.NewLine}" +
            $"    `ColBool` BOOL,{Environment.NewLine}" +
            $"    `ColString` STRING(100),{Environment.NewLine}" +
            $"    `ColStringMax` STRING(MAX),{Environment.NewLine}" +
            $"    `ColChar` STRING(1),{Environment.NewLine}" +
            $"    `ColBytes` BYTES(100),{Environment.NewLine}" +
            $"    `ColBytesMax` BYTES(MAX),{Environment.NewLine}" +
            $"    `ColDate` DATE,{Environment.NewLine}" +
            $"    `ColTimestamp` TIMESTAMP,{Environment.NewLine}" +
            $"    `ColCommitTS` TIMESTAMP OPTIONS (allow_commit_timestamp=true) ,{Environment.NewLine}" +
            $"    `ColInt64Array` ARRAY<INT64>,{Environment.NewLine}" +
            $"    `ColFloat64Array` ARRAY<FLOAT64>,{Environment.NewLine}" +
            $"    `ColNumericArray` ARRAY<NUMERIC>,{Environment.NewLine}" +
            $"    `ColBoolArray` ARRAY<BOOL>,{Environment.NewLine}" +
            $"    `ColStringArray` ARRAY<STRING(100)>,{Environment.NewLine}" +
            $"    `ColStringMaxArray` ARRAY<STRING(MAX)>,{Environment.NewLine}" +
            $"    `ColBytesArray` ARRAY<BYTES(100)>,{Environment.NewLine}" +
            $"    `ColBytesMaxArray` ARRAY<BYTES(MAX)>,{Environment.NewLine}" +
            $"    `ColDateArray` ARRAY<DATE>,{Environment.NewLine}" +
            $"    `ColTimestampArray` ARRAY<TIMESTAMP>,{Environment.NewLine}" +
            $"    `ColGuid` STRING(36),{Environment.NewLine}" +
            $"    `ColComputed` STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED{Environment.NewLine}" +
            $")PRIMARY KEY (`ColSequence`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `TicketSales` ({Environment.NewLine}" +
            $"    `Id` INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),{Environment.NewLine}" +
            $"    `CustomerName` STRING(MAX){Environment.NewLine}" +
            $")PRIMARY KEY (`Id`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Venues` ({Environment.NewLine}" +
            $"    `Code` STRING(10) NOT NULL,{Environment.NewLine}" +
            $"    `Name` STRING(100),{Environment.NewLine}" +
            $"    `Active` BOOL NOT NULL,{Environment.NewLine}" +
            $"    `Capacity` INT64,{Environment.NewLine}" +
            $"    `Ratings` ARRAY<FLOAT64>{Environment.NewLine}" +
            $")PRIMARY KEY (`Code`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Albums` ({Environment.NewLine}" +
            $"    `AlbumId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Title` STRING(100) NOT NULL,{Environment.NewLine}" +
            $"    `ReleaseDate` DATE,{Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `MarketingBudget` INT64,{Environment.NewLine}" +
            $" CONSTRAINT `FK_Albums_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`),{Environment.NewLine}" +
            $")PRIMARY KEY (`AlbumId`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Concerts` ({Environment.NewLine}" +
            $"    `VenueCode` STRING(10) NOT NULL,{Environment.NewLine}" +
            $"    `StartTime` TIMESTAMP NOT NULL,{Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Title` STRING(200),{Environment.NewLine}" +
            $" CONSTRAINT `FK_Concerts_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`) ON DELETE CASCADE,{Environment.NewLine}" +
            $" CONSTRAINT `FK_Concerts_Venues` FOREIGN KEY (`VenueCode`) REFERENCES `Venues` (`Code`),{Environment.NewLine}" +
            $")PRIMARY KEY (`VenueCode`, `StartTime`, `SingerId`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Tracks` ({Environment.NewLine}" +
            $"    `AlbumId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `TrackId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Title` STRING(200) NOT NULL,{Environment.NewLine}" +
            $"    `Duration` NUMERIC,{Environment.NewLine}" +
            $"    `LyricsLanguages` ARRAY<STRING(2)>,{Environment.NewLine}" +
            $"    `Lyrics` ARRAY<STRING(MAX)>,{Environment.NewLine}" +
            $"CONSTRAINT `Chk_Languages_Lyrics_Length_Equal` CHECK (ARRAY_LENGTH(LyricsLanguages) = ARRAY_LENGTH(Lyrics)),{Environment.NewLine}" +
            $")PRIMARY KEY (`AlbumId`, `TrackId`),{Environment.NewLine}" +
            $" INTERLEAVE IN PARENT `Albums` ON DELETE NO ACTION {Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Performances` ({Environment.NewLine}" +
            $"    `VenueCode` STRING(10) NOT NULL,{Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `StartTime` TIMESTAMP NOT NULL,{Environment.NewLine}" +
            $"    `ConcertStartTime` TIMESTAMP NOT NULL,{Environment.NewLine}" +
            $"    `AlbumId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `TrackId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Rating` FLOAT64,{Environment.NewLine}" +
            $" CONSTRAINT `FK_Performances_Concerts` FOREIGN KEY (`VenueCode`, `ConcertStartTime`, `SingerId`) REFERENCES `Concerts` (`VenueCode`, `StartTime`, `SingerId`),{Environment.NewLine}" +
            $" CONSTRAINT `FK_Performances_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`),{Environment.NewLine}" +
            $" CONSTRAINT `FK_Performances_Tracks` FOREIGN KEY (`AlbumId`, `TrackId`) REFERENCES `Tracks` (`AlbumId`, `TrackId`),{Environment.NewLine}" +
            $")PRIMARY KEY (`VenueCode`, `SingerId`, `StartTime`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `AlbumsByAlbumTitle2` ON `Albums` (`Title`) STORING (`MarketingBudget`, `ReleaseDate`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `AlbumsBySingerIdReleaseDateMarketingBudgetTitle` ON `Albums` (`SingerId`, `ReleaseDate` DESC, `MarketingBudget` DESC, `Title`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `idx_concerts_singerId_startTime` ON `Concerts` (`SingerId`, `StartTime`),{Environment.NewLine}" +
            $" INTERLEAVE IN `Singers`{Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `Idx_Singers_FullName` ON `Singers` (`FullName`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE NULL_FILTERED INDEX `IDX_TableWithAllColumnTypes_ColDate_ColCommitTS` ON `TableWithAllColumnTypes` (`ColDate`, `ColCommitTS`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE UNIQUE INDEX `Idx_Tracks_AlbumId_Title` ON `Tracks` (`TrackId`, `Title`){Environment.NewLine}" +
            $"{Environment.NewLine}";
            Assert.Equal(script, generatedScript);
        }
        
        [Fact]
        public void GenerateCreateScriptWithAutoIncrement()
        {
            using var db = new MockMigrationAutoIncrementSampleDbContext();
            var generatedScript = db.Database.GenerateCreateScript();
            var script = $"CREATE TABLE `Singers` ({Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `FirstName` STRING(200),{Environment.NewLine}" +
            $"    `LastName` STRING(200) NOT NULL,{Environment.NewLine}" +
            $"    `FullName` STRING(400) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED,{Environment.NewLine}" +
            $"    `BirthDate` DATE,{Environment.NewLine}" +
            $"    `Picture` BYTES(MAX){Environment.NewLine}" +
            $")PRIMARY KEY (`SingerId`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `TableWithAllColumnTypes` ({Environment.NewLine}" +
            $"    `ColSequence` INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE MySequence)),{Environment.NewLine}" +
            $"    `ColInt64` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `ColFloat64` FLOAT64,{Environment.NewLine}" +
            $"    `ColNumeric` NUMERIC,{Environment.NewLine}" +
            $"    `ColBool` BOOL,{Environment.NewLine}" +
            $"    `ColString` STRING(100),{Environment.NewLine}" +
            $"    `ColStringMax` STRING(MAX),{Environment.NewLine}" +
            $"    `ColChar` STRING(1),{Environment.NewLine}" +
            $"    `ColBytes` BYTES(100),{Environment.NewLine}" +
            $"    `ColBytesMax` BYTES(MAX),{Environment.NewLine}" +
            $"    `ColDate` DATE,{Environment.NewLine}" +
            $"    `ColTimestamp` TIMESTAMP,{Environment.NewLine}" +
            $"    `ColCommitTS` TIMESTAMP OPTIONS (allow_commit_timestamp=true) ,{Environment.NewLine}" +
            $"    `ColInt64Array` ARRAY<INT64>,{Environment.NewLine}" +
            $"    `ColFloat64Array` ARRAY<FLOAT64>,{Environment.NewLine}" +
            $"    `ColNumericArray` ARRAY<NUMERIC>,{Environment.NewLine}" +
            $"    `ColBoolArray` ARRAY<BOOL>,{Environment.NewLine}" +
            $"    `ColStringArray` ARRAY<STRING(100)>,{Environment.NewLine}" +
            $"    `ColStringMaxArray` ARRAY<STRING(MAX)>,{Environment.NewLine}" +
            $"    `ColBytesArray` ARRAY<BYTES(100)>,{Environment.NewLine}" +
            $"    `ColBytesMaxArray` ARRAY<BYTES(MAX)>,{Environment.NewLine}" +
            $"    `ColDateArray` ARRAY<DATE>,{Environment.NewLine}" +
            $"    `ColTimestampArray` ARRAY<TIMESTAMP>,{Environment.NewLine}" +
            $"    `ColGuid` STRING(36),{Environment.NewLine}" +
            $"    `ColComputed` STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED{Environment.NewLine}" +
            $")PRIMARY KEY (`ColSequence`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `TicketSales` ({Environment.NewLine}" +
            $"    `Id` INT64 NOT NULL AUTO_INCREMENT,{Environment.NewLine}" +
            $"    `CustomerName` STRING(MAX){Environment.NewLine}" +
            $")PRIMARY KEY (`Id`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Venues` ({Environment.NewLine}" +
            $"    `Code` STRING(10) NOT NULL,{Environment.NewLine}" +
            $"    `Name` STRING(100),{Environment.NewLine}" +
            $"    `Active` BOOL NOT NULL,{Environment.NewLine}" +
            $"    `Capacity` INT64,{Environment.NewLine}" +
            $"    `Ratings` ARRAY<FLOAT64>{Environment.NewLine}" +
            $")PRIMARY KEY (`Code`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Albums` ({Environment.NewLine}" +
            $"    `AlbumId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Title` STRING(100) NOT NULL,{Environment.NewLine}" +
            $"    `ReleaseDate` DATE,{Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `MarketingBudget` INT64,{Environment.NewLine}" +
            $" CONSTRAINT `FK_Albums_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`),{Environment.NewLine}" +
            $")PRIMARY KEY (`AlbumId`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Concerts` ({Environment.NewLine}" +
            $"    `VenueCode` STRING(10) NOT NULL,{Environment.NewLine}" +
            $"    `StartTime` TIMESTAMP NOT NULL,{Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Title` STRING(200),{Environment.NewLine}" +
            $" CONSTRAINT `FK_Concerts_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`) ON DELETE CASCADE,{Environment.NewLine}" +
            $" CONSTRAINT `FK_Concerts_Venues` FOREIGN KEY (`VenueCode`) REFERENCES `Venues` (`Code`),{Environment.NewLine}" +
            $")PRIMARY KEY (`VenueCode`, `StartTime`, `SingerId`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Tracks` ({Environment.NewLine}" +
            $"    `AlbumId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `TrackId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Title` STRING(200) NOT NULL,{Environment.NewLine}" +
            $"    `Duration` NUMERIC,{Environment.NewLine}" +
            $"    `LyricsLanguages` ARRAY<STRING(2)>,{Environment.NewLine}" +
            $"    `Lyrics` ARRAY<STRING(MAX)>,{Environment.NewLine}" +
            $"CONSTRAINT `Chk_Languages_Lyrics_Length_Equal` CHECK (ARRAY_LENGTH(LyricsLanguages) = ARRAY_LENGTH(Lyrics)),{Environment.NewLine}" +
            $")PRIMARY KEY (`AlbumId`, `TrackId`),{Environment.NewLine}" +
            $" INTERLEAVE IN PARENT `Albums` ON DELETE NO ACTION {Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Performances` ({Environment.NewLine}" +
            $"    `VenueCode` STRING(10) NOT NULL,{Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `StartTime` TIMESTAMP NOT NULL,{Environment.NewLine}" +
            $"    `ConcertStartTime` TIMESTAMP NOT NULL,{Environment.NewLine}" +
            $"    `AlbumId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `TrackId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Rating` FLOAT64,{Environment.NewLine}" +
            $" CONSTRAINT `FK_Performances_Concerts` FOREIGN KEY (`VenueCode`, `ConcertStartTime`, `SingerId`) REFERENCES `Concerts` (`VenueCode`, `StartTime`, `SingerId`),{Environment.NewLine}" +
            $" CONSTRAINT `FK_Performances_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`),{Environment.NewLine}" +
            $" CONSTRAINT `FK_Performances_Tracks` FOREIGN KEY (`AlbumId`, `TrackId`) REFERENCES `Tracks` (`AlbumId`, `TrackId`),{Environment.NewLine}" +
            $")PRIMARY KEY (`VenueCode`, `SingerId`, `StartTime`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `AlbumsByAlbumTitle2` ON `Albums` (`Title`) STORING (`MarketingBudget`, `ReleaseDate`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `AlbumsBySingerIdReleaseDateMarketingBudgetTitle` ON `Albums` (`SingerId`, `ReleaseDate` DESC, `MarketingBudget` DESC, `Title`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `idx_concerts_singerId_startTime` ON `Concerts` (`SingerId`, `StartTime`),{Environment.NewLine}" +
            $" INTERLEAVE IN `Singers`{Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `Idx_Singers_FullName` ON `Singers` (`FullName`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE NULL_FILTERED INDEX `IDX_TableWithAllColumnTypes_ColDate_ColCommitTS` ON `TableWithAllColumnTypes` (`ColDate`, `ColCommitTS`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE UNIQUE INDEX `Idx_Tracks_AlbumId_Title` ON `Tracks` (`TrackId`, `Title`){Environment.NewLine}" +
            $"{Environment.NewLine}";
            Assert.Equal(script, generatedScript);
        }
        
        [Fact]
        public void GenerateCreateScriptWithIdentityColumnsDisabled()
        {
            using var db = new MockMigrationDisableIdentityColumnsSampleDbContext();
            var generatedScript = db.Database.GenerateCreateScript();
            var script = $"CREATE TABLE `Singers` ({Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `FirstName` STRING(200),{Environment.NewLine}" +
            $"    `LastName` STRING(200) NOT NULL,{Environment.NewLine}" +
            $"    `FullName` STRING(400) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED,{Environment.NewLine}" +
            $"    `BirthDate` DATE,{Environment.NewLine}" +
            $"    `Picture` BYTES(MAX){Environment.NewLine}" +
            $")PRIMARY KEY (`SingerId`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `TableWithAllColumnTypes` ({Environment.NewLine}" +
            $"    `ColSequence` INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE MySequence)),{Environment.NewLine}" +
            $"    `ColInt64` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `ColFloat64` FLOAT64,{Environment.NewLine}" +
            $"    `ColNumeric` NUMERIC,{Environment.NewLine}" +
            $"    `ColBool` BOOL,{Environment.NewLine}" +
            $"    `ColString` STRING(100),{Environment.NewLine}" +
            $"    `ColStringMax` STRING(MAX),{Environment.NewLine}" +
            $"    `ColChar` STRING(1),{Environment.NewLine}" +
            $"    `ColBytes` BYTES(100),{Environment.NewLine}" +
            $"    `ColBytesMax` BYTES(MAX),{Environment.NewLine}" +
            $"    `ColDate` DATE,{Environment.NewLine}" +
            $"    `ColTimestamp` TIMESTAMP,{Environment.NewLine}" +
            $"    `ColCommitTS` TIMESTAMP OPTIONS (allow_commit_timestamp=true) ,{Environment.NewLine}" +
            $"    `ColInt64Array` ARRAY<INT64>,{Environment.NewLine}" +
            $"    `ColFloat64Array` ARRAY<FLOAT64>,{Environment.NewLine}" +
            $"    `ColNumericArray` ARRAY<NUMERIC>,{Environment.NewLine}" +
            $"    `ColBoolArray` ARRAY<BOOL>,{Environment.NewLine}" +
            $"    `ColStringArray` ARRAY<STRING(100)>,{Environment.NewLine}" +
            $"    `ColStringMaxArray` ARRAY<STRING(MAX)>,{Environment.NewLine}" +
            $"    `ColBytesArray` ARRAY<BYTES(100)>,{Environment.NewLine}" +
            $"    `ColBytesMaxArray` ARRAY<BYTES(MAX)>,{Environment.NewLine}" +
            $"    `ColDateArray` ARRAY<DATE>,{Environment.NewLine}" +
            $"    `ColTimestampArray` ARRAY<TIMESTAMP>,{Environment.NewLine}" +
            $"    `ColGuid` STRING(36),{Environment.NewLine}" +
            $"    `ColComputed` STRING(MAX) AS (ARRAY_TO_STRING(ColStringArray, ',')) STORED{Environment.NewLine}" +
            $")PRIMARY KEY (`ColSequence`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `TicketSales` ({Environment.NewLine}" +
            $"    `Id` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `CustomerName` STRING(MAX){Environment.NewLine}" +
            $")PRIMARY KEY (`Id`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Venues` ({Environment.NewLine}" +
            $"    `Code` STRING(10) NOT NULL,{Environment.NewLine}" +
            $"    `Name` STRING(100),{Environment.NewLine}" +
            $"    `Active` BOOL NOT NULL,{Environment.NewLine}" +
            $"    `Capacity` INT64,{Environment.NewLine}" +
            $"    `Ratings` ARRAY<FLOAT64>{Environment.NewLine}" +
            $")PRIMARY KEY (`Code`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Albums` ({Environment.NewLine}" +
            $"    `AlbumId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Title` STRING(100) NOT NULL,{Environment.NewLine}" +
            $"    `ReleaseDate` DATE,{Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `MarketingBudget` INT64,{Environment.NewLine}" +
            $" CONSTRAINT `FK_Albums_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`),{Environment.NewLine}" +
            $")PRIMARY KEY (`AlbumId`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Concerts` ({Environment.NewLine}" +
            $"    `VenueCode` STRING(10) NOT NULL,{Environment.NewLine}" +
            $"    `StartTime` TIMESTAMP NOT NULL,{Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Title` STRING(200),{Environment.NewLine}" +
            $" CONSTRAINT `FK_Concerts_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`) ON DELETE CASCADE,{Environment.NewLine}" +
            $" CONSTRAINT `FK_Concerts_Venues` FOREIGN KEY (`VenueCode`) REFERENCES `Venues` (`Code`),{Environment.NewLine}" +
            $")PRIMARY KEY (`VenueCode`, `StartTime`, `SingerId`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Tracks` ({Environment.NewLine}" +
            $"    `AlbumId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `TrackId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Title` STRING(200) NOT NULL,{Environment.NewLine}" +
            $"    `Duration` NUMERIC,{Environment.NewLine}" +
            $"    `LyricsLanguages` ARRAY<STRING(2)>,{Environment.NewLine}" +
            $"    `Lyrics` ARRAY<STRING(MAX)>,{Environment.NewLine}" +
            $"CONSTRAINT `Chk_Languages_Lyrics_Length_Equal` CHECK (ARRAY_LENGTH(LyricsLanguages) = ARRAY_LENGTH(Lyrics)),{Environment.NewLine}" +
            $")PRIMARY KEY (`AlbumId`, `TrackId`),{Environment.NewLine}" +
            $" INTERLEAVE IN PARENT `Albums` ON DELETE NO ACTION {Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE TABLE `Performances` ({Environment.NewLine}" +
            $"    `VenueCode` STRING(10) NOT NULL,{Environment.NewLine}" +
            $"    `SingerId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `StartTime` TIMESTAMP NOT NULL,{Environment.NewLine}" +
            $"    `ConcertStartTime` TIMESTAMP NOT NULL,{Environment.NewLine}" +
            $"    `AlbumId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `TrackId` INT64 NOT NULL,{Environment.NewLine}" +
            $"    `Rating` FLOAT64,{Environment.NewLine}" +
            $" CONSTRAINT `FK_Performances_Concerts` FOREIGN KEY (`VenueCode`, `ConcertStartTime`, `SingerId`) REFERENCES `Concerts` (`VenueCode`, `StartTime`, `SingerId`),{Environment.NewLine}" +
            $" CONSTRAINT `FK_Performances_Singers` FOREIGN KEY (`SingerId`) REFERENCES `Singers` (`SingerId`),{Environment.NewLine}" +
            $" CONSTRAINT `FK_Performances_Tracks` FOREIGN KEY (`AlbumId`, `TrackId`) REFERENCES `Tracks` (`AlbumId`, `TrackId`),{Environment.NewLine}" +
            $")PRIMARY KEY (`VenueCode`, `SingerId`, `StartTime`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `AlbumsByAlbumTitle2` ON `Albums` (`Title`) STORING (`MarketingBudget`, `ReleaseDate`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `AlbumsBySingerIdReleaseDateMarketingBudgetTitle` ON `Albums` (`SingerId`, `ReleaseDate` DESC, `MarketingBudget` DESC, `Title`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `idx_concerts_singerId_startTime` ON `Concerts` (`SingerId`, `StartTime`),{Environment.NewLine}" +
            $" INTERLEAVE IN `Singers`{Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE INDEX `Idx_Singers_FullName` ON `Singers` (`FullName`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE NULL_FILTERED INDEX `IDX_TableWithAllColumnTypes_ColDate_ColCommitTS` ON `TableWithAllColumnTypes` (`ColDate`, `ColCommitTS`){Environment.NewLine}" +
            $"{Environment.NewLine}" +
            $"CREATE UNIQUE INDEX `Idx_Tracks_AlbumId_Title` ON `Tracks` (`TrackId`, `Title`){Environment.NewLine}" +
            $"{Environment.NewLine}";
            Assert.Equal(script, generatedScript);
        }
    }
}
