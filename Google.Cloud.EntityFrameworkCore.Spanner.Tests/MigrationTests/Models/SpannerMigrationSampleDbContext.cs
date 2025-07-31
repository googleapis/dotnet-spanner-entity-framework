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
using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using Microsoft.EntityFrameworkCore;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.MigrationTests.Models
{
    public partial class SpannerMigrationSampleDbContext : DbContext
    {
        public SpannerMigrationSampleDbContext()
        {
        }

        public SpannerMigrationSampleDbContext(DbContextOptions<SpannerMigrationSampleDbContext> options)
            : base(options)
        {
        }

        public virtual DbSet<Albums> Albums { get; set; }
        public virtual DbSet<Concerts> Concerts { get; set; }
        public virtual DbSet<Performances> Performances { get; set; }
        public virtual DbSet<Singers> Singers { get; set; }
        public virtual DbSet<TableWithAllColumnTypes> TableWithAllColumnTypes { get; set; }
        public virtual DbSet<Tracks> Tracks { get; set; }
        public virtual DbSet<Venues> Venues { get; set; }
        public virtual DbSet<TicketSales> TicketSales { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Albums>(entity =>
            {
                entity.HasKey(e => e.AlbumId)
                    .HasName("PRIMARY_KEY");

                entity.Property(e => e.AlbumId).ValueGeneratedNever();

                entity.Property(e => e.Title)
                    .IsRequired()
                    .HasMaxLength(100);

                entity.HasIndex(e => e.Title)
                    .HasDatabaseName("AlbumsByAlbumTitle2")
                    .Storing(a => new { a.MarketingBudget, a.ReleaseDate });

                entity.HasOne(d => d.Singer)
                    .WithMany(p => p.Albums)
                    .HasForeignKey(d => d.SingerId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Albums_Singers");
            });

            modelBuilder.Entity<Concerts>(entity =>
            {
                entity.HasKey(e => new { e.VenueCode, e.StartTime, e.SingerId })
                    .HasName("PRIMARY_KEY");

                entity.Property(e => e.VenueCode).HasMaxLength(10);

                entity.Property(e => e.Title).HasMaxLength(200);

                entity.HasOne(d => d.Singer)
                    .WithMany(p => p.Concerts)
                    .HasForeignKey(d => d.SingerId)
                    .OnDelete(DeleteBehavior.Cascade)
                    .HasConstraintName("FK_Concerts_Singers");

                entity.HasOne(d => d.VenueCodeNavigation)
                    .WithMany(p => p.Concerts)
                    .HasForeignKey(d => d.VenueCode)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Concerts_Venues");
            });

            modelBuilder.Entity<Performances>(entity =>
            {
                entity.HasKey(e => new { e.VenueCode, e.SingerId, e.StartTime })
                    .HasName("PRIMARY_KEY");

                entity.Property(e => e.VenueCode).HasMaxLength(10);

                entity.HasOne(d => d.Singer)
                    .WithMany(p => p.Performances)
                    .HasForeignKey(d => d.SingerId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Performances_Singers");

                entity.HasOne(d => d.Tracks)
                    .WithMany(p => p.Performances)
                    .HasForeignKey(d => new { d.AlbumId, d.TrackId })
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Performances_Tracks");

                entity.HasOne(d => d.Concerts)
                    .WithMany(p => p.Performances)
                    .HasForeignKey(d => new { d.VenueCode, d.ConcertStartTime, d.SingerId })
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Performances_Concerts");
            });

            modelBuilder.Entity<Singers>(entity =>
            {
                entity.HasKey(e => e.SingerId)
                    .HasName("PRIMARY_KEY");

                entity.HasIndex(e => e.FullName)
                    .HasDatabaseName("Idx_Singers_FullName");

                entity.Property(e => e.SingerId).ValueGeneratedNever();

                entity.Property(e => e.FirstName).HasMaxLength(200);

                entity.Property(e => e.FullName)
                    .IsRequired()
                    .HasMaxLength(400)
                    .HasComputedColumnSql("(COALESCE(FirstName || ' ', '') || LastName) STORED")
                    .ValueGeneratedOnAddOrUpdate();

                entity.Property(e => e.LastName)
                    .IsRequired()
                    .HasMaxLength(200);

                entity.Property(e => e.Picture);
            });

            modelBuilder.Entity<TableWithAllColumnTypes>(entity =>
            {
                entity.HasKey(e => e.ColSequence)
                    .HasName("PRIMARY_KEY");

                entity.HasIndex(e => new { e.ColDate, e.ColCommitTs })
                    .HasDatabaseName("IDX_TableWithAllColumnTypes_ColDate_ColCommitTS")
                    .IsNullFiltered();

                entity.Property(e => e.ColSequence).ValueGeneratedOnAdd();
                entity.Property(e => e.ColSequence).HasDefaultValueSql("GET_NEXT_SEQUENCE_VALUE(SEQUENCE MySequence)");

                entity.Property(e => e.ColBytes).HasMaxLength(100);

                entity.Property(e => e.ColBytesArray).HasMaxLength(100);

                entity.Property(e => e.ColBytesMax);

                entity.Property(e => e.ColBytesMaxArray);

                entity.Property(e => e.ColCommitTs)
                    .HasColumnName("ColCommitTS")
                    .HasAnnotation("UpdateCommitTimestamp", SpannerUpdateCommitTimestamp.OnInsertAndUpdate);

                entity.Property(e => e.ColComputed)
                    .HasComputedColumnSql("(ARRAY_TO_STRING(ColStringArray, ',')) STORED")
                    .ValueGeneratedOnAddOrUpdate();

                entity.Property(e => e.ColString).HasMaxLength(100);

                entity.Property(e => e.ColStringArray).HasMaxLength(100);

                entity.Property(e => e.ColStringMax);

                entity.Property(e => e.ColStringMaxArray);
            });

            modelBuilder.Entity<Tracks>(entity =>
            {
                entity.InterleaveInParent(typeof(Albums))
                    .HasKey(e => new { e.AlbumId, e.TrackId })
                    .HasName("PRIMARY_KEY");

                entity.ToTable(t => t.HasCheckConstraint("Chk_Languages_Lyrics_Length_Equal",
                    "ARRAY_LENGTH(LyricsLanguages) = ARRAY_LENGTH(Lyrics)"));

                entity.HasIndex(e => new { e.TrackId, e.Title })
                    .HasDatabaseName("Idx_Tracks_AlbumId_Title")
                    .IsUnique();

                entity.Property(e => e.Lyrics);

                entity.Property(e => e.LyricsLanguages).HasMaxLength(2);

                entity.Property(e => e.Title)
                    .IsRequired()
                    .HasMaxLength(200);

                entity.HasOne(d => d.Album)
                    .WithMany(p => p.Tracks)
                    .HasForeignKey(d => d.AlbumId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("PK_Albums");
            });

            modelBuilder.Entity<Venues>(entity =>
            {
                entity.HasKey(e => e.Code)
                    .HasName("PRIMARY_KEY");

                entity.Property(e => e.Code).HasMaxLength(10);

                entity.Property(e => e.Name).HasMaxLength(100);
            });
            modelBuilder.Entity<TicketSales>();
        }
    }
}
