// Copyright 2020 Google LLC
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

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    public partial class MigrationDbContext : DbContext
    {
        public MigrationDbContext()
        {
        }

        public MigrationDbContext(DbContextOptions<MigrationDbContext> options)
            : base(options)
        {
        }

        public DbSet<Product> Products { get; set; }
        public DbSet<Category> Categories { get; set; }
        public DbSet<Order> Orders { get; set; }
        public DbSet<OrderDetail> OrderDetails { get; set; }
        public DbSet<AllColType> AllColTypes { get; set; }
        public DbSet<Article> Articles { get; set; }
        public DbSet<Author> Authors { get; set; }


        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AllColType>(entity =>
            {
                entity.Property(e => e.ColDate).HasColumnType("DATE");
                entity.Property(e => e.ColDateArray).HasColumnType("ARRAY<DATE>");
                entity.Property(e => e.ColDateList).HasColumnType("ARRAY<DATE>");
                entity.Property(e => e.ColCommitTimestamp)
                .HasAnnotation("UpdateCommitTimestamp", SpannerUpdateCommitTimestamp.OnInsertAndUpdate);
            });

            modelBuilder.Entity<OrderDetail>()
                .HasKey(c => new { c.OrderId, c.ProductId });

            modelBuilder.Entity<Article>()
                .HasKey(c => new { c.AuthorId, c.ArticleId });

            modelBuilder.Entity<Product>().ToTable("Products");
            modelBuilder.Entity<Category>().ToTable("Categories");
            modelBuilder.Entity<Order>().ToTable("Orders");
            modelBuilder.Entity<OrderDetail>().ToTable("OrderDetails");
            modelBuilder.Entity<AllColType>().ToTable("AllColTypes");
            modelBuilder.Entity<Article>().ToTable("Articles");
            modelBuilder.Entity<Author>().ToTable("Authors");
        }
    }
}
