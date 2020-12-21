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

// Generated from SampleDataModel.sql

using Microsoft.EntityFrameworkCore;

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

        public virtual DbSet<Addresses> Addresses { get; set; }
        public virtual DbSet<Albums> Albums { get; set; }
        public virtual DbSet<Concerts> Concerts { get; set; }
        public virtual DbSet<Performances> Performances { get; set; }
        public virtual DbSet<Singers> Singers { get; set; }
        public virtual DbSet<TableWithAllColumnTypes> TableWithAllColumnTypes { get; set; }
        public virtual DbSet<Tracks> Tracks { get; set; }
        public virtual DbSet<Venues> Venues { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Addresses>(entity =>
            {
                entity.HasKey(e => new { e.Address, e.Country })
                    .HasName("PRIMARY_KEY");

                entity.Property(e => e.Address).HasMaxLength(100);

                entity.Property(e => e.Country).HasMaxLength(100);
            });

            modelBuilder.Entity<Albums>(entity =>
            {
                entity.HasKey(e => e.AlbumId)
                    .HasName("PRIMARY_KEY");

                entity.HasIndex(e => e.Singer)
                    .HasName("IDX_Albums_Singer_F23D326B3F7F072E");

                entity.Property(e => e.AlbumId).ValueGeneratedNever();

                entity.Property(e => e.ReleaseDate).HasColumnType("DATE");

                entity.Property(e => e.Title)
                    .IsRequired()
                    .HasMaxLength(100);

                entity.HasOne(d => d.SingerNavigation)
                    .WithMany(p => p.Albums)
                    .HasForeignKey(d => d.Singer)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Albums_Singers");
            });

            modelBuilder.Entity<Concerts>(entity =>
            {
                entity.HasKey(e => new { e.Venue, e.StartTime, e.SingerId })
                    .HasName("PRIMARY_KEY");

                entity.HasIndex(e => e.SingerId)
                    .HasName("IDX_Concerts_SingerId_B428E23F69F5F316");

                entity.Property(e => e.Venue).HasMaxLength(10);

                entity.Property(e => e.Title).HasMaxLength(200);

                entity.HasOne(d => d.Singer)
                    .WithMany(p => p.Concerts)
                    .HasForeignKey(d => d.SingerId)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Concerts_Singers");

                entity.HasOne(d => d.VenueNavigation)
                    .WithMany(p => p.Concerts)
                    .HasForeignKey(d => d.Venue)
                    .OnDelete(DeleteBehavior.ClientSetNull)
                    .HasConstraintName("FK_Concerts_Venues");
            });

            modelBuilder.Entity<Performances>(entity =>
            {
                entity.HasKey(e => new { e.Venue, e.SingerId, e.StartTime })
                    .HasName("PRIMARY_KEY");

                entity.HasIndex(e => e.SingerId)
                    .HasName("IDX_Performances_SingerId_027098E475ABA8F2");

                entity.HasIndex(e => new { e.AlbumId, e.TrackId })
                    .HasName("IDX_Performances_AlbumId_TrackId_E337390ADF11835E");

                entity.HasIndex(e => new { e.Venue, e.ConcertStartTime, e.SingerId })
                    .HasName("IDX_Performances_Venue_ConcertStartTime_SingerId_984D85F4C1A39212");

                entity.Property(e => e.Venue).HasMaxLength(10);

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
                    .HasForeignKey(d => new { d.Venue, d.ConcertStartTime, d.SingerId })
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

                entity.Property(e => e.BirthDate).HasColumnType("DATE");

                entity.Property(e => e.FirstName).HasMaxLength(200);

                entity.Property(e => e.FullName)
                    .HasMaxLength(400)
                    .IsRequired();

                entity.Property(e => e.LastName)
                    .IsRequired()
                    .HasMaxLength(200);

                entity.Property(e => e.Picture).HasColumnType("BYTES(MAX)");
            });

            modelBuilder.Entity<TableWithAllColumnTypes>(entity =>
            {
                entity.HasKey(e => e.ColInt64)
                    .HasName("PRIMARY_KEY");

                entity.Property(e => e.ColInt64).ValueGeneratedNever();

                entity.Property(e => e.ColBoolArray).HasColumnType("ARRAY<BOOL>");

                entity.Property(e => e.ColBytes)
                    .IsRequired()
                    .HasColumnType("BYTES(100)");

                entity.Property(e => e.ColBytesArray).HasColumnType("ARRAY<BYTES(100)>");

                entity.Property(e => e.ColBytesMax)
                    .IsRequired()
                    .HasColumnType("BYTES(MAX)");

                entity.Property(e => e.ColBytesMaxArray).HasColumnType("ARRAY<BYTES(MAX)>");

                entity.Property(e => e.ColCommitTs).HasColumnName("ColCommitTS");

                entity.Property(e => e.ColComputed).HasColumnType("STRING(MAX)");

                entity.Property(e => e.ColDate).HasColumnType("DATE");

                entity.Property(e => e.ColDateArray).HasColumnType("ARRAY<DATE>");

                entity.Property(e => e.ColFloat64Array).HasColumnType("ARRAY<FLOAT64>");

                entity.Property(e => e.ColInt64Array).HasColumnType("ARRAY<INT64>");

                entity.Property(e => e.ColString)
                    .IsRequired()
                    .HasMaxLength(100);

                entity.Property(e => e.ColStringArray).HasColumnType("ARRAY<STRING(100)>");

                entity.Property(e => e.ColStringMax)
                    .IsRequired()
                    .HasColumnType("STRING(MAX)");

                entity.Property(e => e.ColStringMaxArray).HasColumnType("ARRAY<STRING(MAX)>");

                entity.Property(e => e.ColTimestampArray).HasColumnType("ARRAY<TIMESTAMP>");
            });

            modelBuilder.Entity<Tracks>(entity =>
            {
                entity.HasKey(e => new { e.AlbumId, e.TrackId })
                    .HasName("PRIMARY_KEY");

                entity.HasIndex(e => new { e.TrackId, e.Title })
                    .HasName("Idx_Tracks_AlbumId_Title");

                entity.Property(e => e.Duration).HasColumnType("NUMERIC");

                entity.Property(e => e.Lyrics).HasColumnType("ARRAY<STRING(MAX)>");

                entity.Property(e => e.LyricsLanguages).HasColumnType("ARRAY<STRING(2)>");

                entity.Property(e => e.Title)
                    .IsRequired()
                    .HasMaxLength(200);
            });

            modelBuilder.Entity<Venues>(entity =>
            {
                entity.HasKey(e => e.Code)
                    .HasName("PRIMARY_KEY");

                entity.Property(e => e.Code).HasMaxLength(10);

                entity.Property(e => e.Name).HasMaxLength(100);

                entity.Property(e => e.Ratings).HasColumnType("ARRAY<FLOAT64>");
            });

            OnModelCreatingPartial(modelBuilder);
        }

        partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
    }
}
