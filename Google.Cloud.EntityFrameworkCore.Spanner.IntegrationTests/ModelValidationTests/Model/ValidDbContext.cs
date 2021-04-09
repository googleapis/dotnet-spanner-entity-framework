// Copyright 2021 Google Inc. All Rights Reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.Spanner.Common.V1;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.ModelValidationTests
{
    public class ValidDbContext : DbContext
    {
        private readonly DatabaseName _databaseName;

        public ValidDbContext(DatabaseName databaseName) => _databaseName = databaseName;

        public virtual DbSet<Singer> Singers { get; set; }
        public virtual DbSet<Album> Albums { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Singer>(entity =>
            {
                entity.HasKey(e => e.SingerId);
            });

            modelBuilder.Entity<Album>(entity =>
            {
                entity
                    .InterleaveInParent(typeof(Singer), OnDelete.Cascade)
                    .HasKey(entity => new { entity.SingerId, entity.AlbumId });
            });
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseLazyLoadingProxies()
                    .UseSpanner($"Data Source={_databaseName};emulatordetection=EmulatorOrProduction");
            }
        }
    }
}
