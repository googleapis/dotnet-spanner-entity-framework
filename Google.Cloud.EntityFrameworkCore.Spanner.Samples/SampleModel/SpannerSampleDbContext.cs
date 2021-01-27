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
using Microsoft.EntityFrameworkCore.ChangeTracking;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel
{
    /// <summary>
    /// Sample DatabaseContext for entities that can use a client side version number as concurrency token.
    /// It is recommended to use client side generated values with Cloud Spanner, as Cloud Spanner does not
    /// have any server side functions that can efficiently be used for this. Commit timestamps could in theory
    /// be used, but the problem with commit timestamps is that using PENDING_COMMIT_TIMESTAMP() in a DML
    /// statement renders the entire table of the DML statement to be unreadable for the remainder of the transaction.
    /// 
    /// Other server side functions will always require an additional round-trip to the server to fetch the generated
    /// value. Using a simple client side version number that is steadily increasing is the same strategy that also
    /// Hibernate (Java) uses, and has proven to be a robust yet simple implementation for optimistic locking.
    /// </summary>
    public partial class SpannerSampleDbContext : DbContext
    {
        private readonly string _connectionString;

        public SpannerSampleDbContext(string connectionString)
        {
            _connectionString = connectionString;
        }

        public SpannerSampleDbContext(string connectionString, DbContextOptions<SpannerSampleDbContext> options)
            : base(options)
        {
            _connectionString = connectionString;
        }

        public virtual DbSet<Singer> Singers { get; set; }
        public virtual DbSet<Album> Albums { get; set; }
        public virtual DbSet<Track> Tracks { get; set; }
        public virtual DbSet<Venue> Venues { get; set; }
        public virtual DbSet<Concert> Concerts { get; set; }
        public virtual DbSet<Performance> Performances { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options.UseSpanner(_connectionString);

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Singer>(entity =>
            {
                entity.HasKey(e => e.SingerId);
                entity.Property(e => e.Version).IsConcurrencyToken();
            });

            modelBuilder.Entity<Album>(entity =>
            {
                entity.HasKey(entity => new { entity.AlbumId });
                entity.Property(e => e.Version).IsConcurrencyToken();
            });

            modelBuilder.Entity<Track>(entity =>
            {
                entity.HasKey(entity => new { entity.AlbumId, entity.TrackId });
                entity.Property(e => e.Version).IsConcurrencyToken();
            });

            modelBuilder.Entity<Venue>(entity =>
            {
                entity.HasKey(entity => new { entity.Code });
                entity.Property(e => e.Version).IsConcurrencyToken();
            });

            modelBuilder.Entity<Concert>(entity =>
            {
                entity.HasKey(entity => new { entity.VenueCode, entity.SingerId, entity.StartTime });
                entity.Property(e => e.Version).IsConcurrencyToken();
            });

            modelBuilder.Entity<Performance>(entity =>
            {
                entity.HasKey(entity => new { entity.VenueCode, entity.SingerId, entity.ConcertStartTime });
                entity.Property(e => e.Version).IsConcurrencyToken();
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);

        public override int SaveChanges(bool acceptAllChangesOnSuccess)
        {
            UpdateVersions();
            return base.SaveChanges(acceptAllChangesOnSuccess);
        }

        public override Task<int> SaveChangesAsync(
            bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default)
        {
            UpdateVersions();
            return base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
        }

        private void UpdateVersions()
        {
            foreach (EntityEntry entityEntry in ChangeTracker.Entries())
            {
                if (entityEntry.Entity is VersionedEntity versionedEntity)
                {
                    var propertyEntry = entityEntry.Property(nameof(versionedEntity.Version));
                    var currentVersion = (long)propertyEntry.CurrentValue;
                    propertyEntry.CurrentValue = currentVersion + 1L;
                }
            }
        }
    }
}
