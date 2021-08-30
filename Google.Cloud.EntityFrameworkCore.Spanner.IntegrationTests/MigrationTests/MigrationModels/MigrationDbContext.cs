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
                entity.HasIndex(e => new { e.ColDate, e.ColCommitTimestamp })
                .IsNullFiltered();
                entity.Property(e => e.ColCommitTimestamp)
                .HasAnnotation("UpdateCommitTimestamp", SpannerUpdateCommitTimestamp.OnInsertAndUpdate);
            });

            modelBuilder.Entity<OrderDetail>()
                .HasKey(c => new { c.OrderId, c.ProductId });

            modelBuilder.Entity<Article>()
                .InterleaveInParent(typeof(Author), OnDelete.Cascade)
                .HasKey(c => new { c.AuthorId, c.ArticleId });

            modelBuilder.Entity<Author>()
                .Property(c => c.FullName)
                .ValueGeneratedOnAddOrUpdate()
                .HasComputedColumnSql("(ARRAY_TO_STRING([FirstName, LastName], ' ')) STORED");

            modelBuilder.Entity<Author>().HasData(
                new Author { AuthorId = 1, FirstName = "Belinda", LastName = "Stiles" },
                new Author { AuthorId = 2, FirstName = "Kelly", LastName = "Houser" });
        }
    }
}
