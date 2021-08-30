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

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests.Migrations
{
    [DbContext(typeof(MockMigrationSampleDbContext))]
    [Migration("20210830_V2")]
    public class V2 : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            // Drop the TableWithAllColumnTypes table, including the related index.
            migrationBuilder.DropIndex("IDX_TableWithAllColumnTypes_ColDate_ColCommitTS", "TableWithAllColumnTypes");
            migrationBuilder.DropTable("TableWithAllColumnTypes");
        }
    }
}
