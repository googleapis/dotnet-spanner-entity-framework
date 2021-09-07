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

using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model
{
    public partial class SpannerSampleDbContext : DbContext
    {
        public SpannerSampleDbContext()
        {
        }

        public SpannerSampleDbContext(DbContextOptions<SpannerSampleDbContext> options)
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
                    .OnDelete(DeleteBehavior.ClientSetNull)
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
                    .HasName("Idx_Singers_FullName");

                entity.Property(e => e.SingerId).ValueGeneratedNever();

                entity.Property(e => e.FirstName).HasMaxLength(200);

                entity.Property(e => e.FullName)
                    .IsRequired()
                    .HasMaxLength(400)
                    .ValueGeneratedOnAddOrUpdate();

                entity.Property(e => e.LastName)
                    .IsRequired()
                    .HasMaxLength(200);

                entity.Property(e => e.Picture).HasColumnType("BYTES(10485760)");
            });

            modelBuilder.Entity<TableWithAllColumnTypes>(entity =>
            {
                entity.HasKey(e => e.ColInt64)
                    .HasName("PRIMARY_KEY");

                entity.HasIndex(e => new { e.ColDate, e.ColCommitTs })
                    .HasName("IDX_TableWithAllColumnTypes_ColDate_ColCommitTS")
                    .HasAnnotation("Spanner:IsNullFiltered", true);

                entity.Property(e => e.ColInt64).ValueGeneratedNever();

                entity.Property(e => e.ColBytes).HasColumnType("BYTES(100)");

                entity.Property(e => e.ColBytesArray).HasColumnType("ARRAY<BYTES(100)>");

                entity.Property(e => e.ColBytesMax).HasColumnType("BYTES(10485760)");

                entity.Property(e => e.ColBytesMaxArray).HasColumnType("ARRAY<BYTES(10485760)>");

                entity.Property(e => e.ColCommitTs)
                    .HasColumnName("ColCommitTS")
                    .HasAnnotation("UpdateCommitTimestamp", SpannerUpdateCommitTimestamp.OnInsertAndUpdate);

                entity.Property(e => e.ColComputed)
                    .HasMaxLength(2621440)
                    .ValueGeneratedOnAddOrUpdate();

                entity.Property(e => e.ColString).HasMaxLength(100);

                entity.Property(e => e.ColStringArray).HasColumnType("ARRAY<STRING(100)>");

                entity.Property(e => e.ColStringMax).HasMaxLength(2621440);

                entity.Property(e => e.ColStringMaxArray).HasColumnType("ARRAY<STRING(2621440)>");
            });

            modelBuilder.Entity<Tracks>(entity =>
            {
                entity.HasKey(e => new { e.AlbumId, e.TrackId })
                    .HasName("PRIMARY_KEY");

                entity.HasAnnotation("CONSTRAINT `Chk_Languages_Lyrics_Length_Equal`", "CHECK ARRAY_LENGTH(LyricsLanguages) = ARRAY_LENGTH(Lyrics)");

                entity.HasIndex(e => new { e.TrackId, e.Title })
                    .HasName("Idx_Tracks_AlbumId_Title")
                    .IsUnique();

                entity.Property(e => e.Lyrics).HasColumnType("ARRAY<STRING(2621440)>");

                entity.Property(e => e.LyricsLanguages).HasColumnType("ARRAY<STRING(2)>");

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

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}
