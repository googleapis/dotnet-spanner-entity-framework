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
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests.Models;
using Microsoft.EntityFrameworkCore;
using System.IO;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests
{
    internal class MockMigrationSampleDbContext : SpannerMigrationSampleDbContext
    {
        public MockMigrationSampleDbContext()
        {
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseSpanner("Data Source=projects/p1/instances/i1/databases/d1;", _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false));
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
            var script = File.ReadAllText("MigrationTests/SampleMigrationDataModel.sql");
            Assert.Equal(script, generatedScript);
        }
    }
}
