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

using Google.Cloud.Spanner.Admin.Database.V1;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using System;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests
{
    /// <summary>
    /// Tests migrations using an in-mem Spanner mock server.
    /// </summary>
    [CollectionDefinition(nameof(MigrationMockServerTests), DisableParallelization = true)]
    [Collection(nameof(MigrationMockServerTests))]
    public class MigrationMockServerTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public MigrationMockServerTests(SpannerMockServerFixture service)
        {
            _fixture = service;
            service.SpannerMock.Reset();
        }

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        [Fact]
        public void TestMigrateUsesDdlBatch()
        {
            var version = typeof(Migration).Assembly.GetName().Version ?? new Version();
            var formattedVersion = $"{version.Major}.{version.Minor}.{version.Build}";
            _fixture.SpannerMock.AddOrUpdateStatementResult("SELECT 1", StatementResult.CreateException(MockSpannerService.CreateDatabaseNotFoundException("d1")));
            _fixture.SpannerMock.AddOrUpdateStatementResult(
                $"INSERT INTO `EFMigrationsHistory` (`MigrationId`, `ProductVersion`)\nVALUES ('20210309110233_Initial', '{formattedVersion}')",
                StatementResult.CreateUpdateCount(1)
            );
            _fixture.SpannerMock.AddOrUpdateStatementResult(
                $"INSERT INTO `EFMigrationsHistory` (`MigrationId`, `ProductVersion`)\nVALUES ('20210830_V2', '{formattedVersion}')",
                StatementResult.CreateUpdateCount(1)
            );
            using var db = new MockMigrationSampleDbContext(ConnectionString);
            db.Database.Migrate();
            
            Assert.Collection(_fixture.DatabaseAdminMock.Requests,
                // The initial request will create an empty database and then create the migrations history table.
                request => Assert.IsType<CreateDatabaseRequest>(request),
                request =>
                {
                    var update = request as UpdateDatabaseDdlRequest;
                    Assert.NotNull(update);
                    Assert.Collection(update.Statements,
                        sql => Assert.StartsWith("CREATE TABLE `EFMigrationsHistory`", sql)
                    );
                },
                // Each migration will be executed as a separate DDL batch.
                request =>
                {
                    var update = request as UpdateDatabaseDdlRequest;
                    Assert.NotNull(update);
                    Assert.Collection(update.Statements,
                        sql => Assert.StartsWith("CREATE TABLE `Singers`", sql),
                        sql => Assert.StartsWith("CREATE TABLE `TableWithAllColumnTypes`", sql),
                        sql => Assert.StartsWith("CREATE TABLE `Venues`", sql),
                        sql => Assert.StartsWith("CREATE TABLE `Albums`", sql),
                        sql => Assert.StartsWith("CREATE TABLE `Concerts`", sql),
                        sql => Assert.StartsWith("CREATE TABLE `Tracks`", sql),
                        sql => Assert.StartsWith("CREATE TABLE `Performances`", sql),
                        sql => Assert.StartsWith("CREATE INDEX `AlbumsByAlbumTitle2`", sql),
                        sql => Assert.StartsWith("CREATE INDEX `Idx_Singers_FullName`", sql),
                        sql => Assert.StartsWith("CREATE NULL_FILTERED INDEX `IDX_TableWithAllColumnTypes_ColDate_ColCommitTS`", sql),
                        sql => Assert.StartsWith("CREATE UNIQUE INDEX `Idx_Tracks_AlbumId_Title`", sql)
                    );
                },
                request =>
                {
                    var update = request as UpdateDatabaseDdlRequest;
                    Assert.NotNull(update);
                    Assert.Collection(update.Statements,
                        sql => Assert.StartsWith(" DROP INDEX `IDX_TableWithAllColumnTypes_ColDate_ColCommitTS`", sql),
                        sql => Assert.StartsWith("DROP TABLE `TableWithAllColumnTypes`", sql)
                    );
                }
            );
        }
    }
}
