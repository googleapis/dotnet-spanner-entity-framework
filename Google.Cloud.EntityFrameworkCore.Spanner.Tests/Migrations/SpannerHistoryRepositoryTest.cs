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

using Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using System;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Migrations
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class SpannerHistoryRepositoryTest
    {
        private static string EOL => Environment.NewLine;

        [Fact]
        public void GetCreateScript_works()
        {
            var sql = CreateHistoryRepository().GetCreateScript();

            Assert.Equal(
                "CREATE TABLE `EFMigrationsHistory` ("
                + EOL
                + "    `MigrationId` STRING(150) NOT NULL,"
                + EOL
                + "    `ProductVersion` STRING(32) NOT NULL"
                + EOL
                + ")PRIMARY KEY (`MigrationId`)",
                sql);
        }

        private static IHistoryRepository CreateHistoryRepository(string schema = null)
            => new DbContext(
                new DbContextOptionsBuilder()
                .UseInternalServiceProvider(SpannerTestHelpers.Instance.CreateServiceProvider())
                .UseSpanner(
                    new SpannerConnection("Data Source=projects/p1/instances/i1/databases/d1"),

                    b => b.MigrationsHistoryTable(SpannerHistoryRepository.DefaultMigrationsHistoryTableName, schema))
                .Options)
        .GetService<IHistoryRepository>();
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
