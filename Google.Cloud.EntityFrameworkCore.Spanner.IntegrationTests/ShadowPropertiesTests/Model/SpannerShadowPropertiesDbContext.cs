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

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.ShadowPropertiesModel
{
    public partial class SpannerShadowPropertiesDbContext : DbContext
    {
        public SpannerShadowPropertiesDbContext()
        {
        }

        public SpannerShadowPropertiesDbContext(DbContextOptions<SpannerShadowPropertiesDbContext> options)
            : base(options)
        {
        }

        public virtual DbSet<Singer> Singers { get; set; }
        public virtual DbSet<Album> Albums { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Singer>(entity =>
            {
                entity
                    .Property<DateTime>("LastModified")
                    .HasAnnotation("UpdateCommitTimestamp", SpannerUpdateCommitTimestamp.OnInsertAndUpdate);
            });

            modelBuilder.Entity<Album>(entity =>
            {
                entity
                    .Property<DateTime>("LastModified")
                    .HasAnnotation("UpdateCommitTimestamp", SpannerUpdateCommitTimestamp.OnInsertAndUpdate);
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}
