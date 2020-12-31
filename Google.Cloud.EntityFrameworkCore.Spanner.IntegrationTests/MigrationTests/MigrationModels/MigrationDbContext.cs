using Google.Cloud.Spanner.Common.V1;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Text;

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

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<OrderDetail>()
                .HasKey(c => new { c.OrderId, c.ProductId });

            modelBuilder.Entity<Product>().ToTable("Products");
            modelBuilder.Entity<Brand>().ToTable("Brands");
            modelBuilder.Entity<Order>().ToTable("Orders");
            modelBuilder.Entity<OrderDetail>().ToTable("OrderDetails");
        }

        public DbSet<Product> Products { get; set; }
        public DbSet<Brand> Brands { get; set; }
        public DbSet<Order> Orders { get; set; }
        public DbSet<OrderDetail> OrderDetails { get; set; }
    }
}
