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

using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.Spanner.Data;
using System;
using System.Collections.Generic;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    [Collection(nameof(SpannerSampleFixture))]
    public class ScaffoldingTests
    {
        private readonly SpannerSampleFixture _fixture;

        public ScaffoldingTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [Fact]
        public async void AllTablesAreGenerated()
        {
            using (var connection = _fixture.GetConnection())
            {
                var tableNames = new string[] {
                    "Singers", "Albums", "Tracks", "Venues", "Concerts", "Performances", "TableWithAllColumnTypes"
                };
                var tables = new SpannerParameterCollection();
                tables.Add("tables", SpannerDbType.ArrayOf(SpannerDbType.String), tableNames);
                var cmd = connection.CreateSelectCommand(
                    "SELECT COUNT(*) " +
                    "FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_CATALOG='' AND TABLE_SCHEMA='' AND TABLE_NAME IN UNNEST (@tables)", tables);
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    Assert.True(await reader.ReadAsync());
                    Assert.Equal(tableNames.Length, reader.GetInt64(0));
                    Assert.False(await reader.ReadAsync());
                }
            }
        }

        [Fact]
        public async void CanInsertVenue()
        {
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var venue = new Venues
                {
                    Code = "CON",
                    Name = "Concert Hall",
                    Active = true,
                    Capacity = 2000,
                    Ratings = new List<double> { 8.9, 6.5, 8.0 },
                };
                db.Venues.Add(venue);
                await db.SaveChangesAsync();
            }
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the venue from the database.
                var refreshedVenue = await db.Venues.FindAsync("CON");
                Assert.Equal("Concert Hall", refreshedVenue.Name);
            }
        }

        [Fact]
        public async void CanInsertSinger()
        {
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var singer = new Singers
                {
                    SingerId = 1,
                    FirstName = "Rob",
                    LastName = "Morrison",
                    BirthDate = new System.DateTime(2002, 10, 1),
                    Picture = new byte[] { 1, 2, 3 },
                };
                db.Singers.Add(singer);
                await db.SaveChangesAsync();

                // Check that the calculated field was also automatically updated.
                Assert.Equal("Rob Morrison", singer.FullName);
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the singer from the database.
                var singer = await db.Singers.FindAsync(1L);
                Assert.Equal("Rob Morrison", singer.FullName);
            }
        }

        [Fact]
        public async void CanInsertAlbum()
        {
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var singer = new Singers
                {
                    SingerId = 2,
                    FirstName = "Pete",
                    LastName = "Henderson",
                    BirthDate = new DateTime(1997, 2, 20),
                };
                db.Singers.Add(singer);
                var album = new Albums
                {
                    SingerId = singer.SingerId,
                    AlbumId = 1,
                    Title = "Pete Henderson's first album",
                    ReleaseDate = new DateTime(2019, 04, 19),
                };
                db.Albums.Add(album);
                await db.SaveChangesAsync();

                // Check that we can use the Singer reference.
                Assert.Equal("Pete Henderson", album.Singer.FullName);
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the album from the database.
                var album = await db.Albums.FindAsync(1L);
                Assert.Equal("Pete Henderson's first album", album.Title);
                Assert.NotNull(album.Singer);
            }
        }

        [Fact]
        public async void CanInsertTrack()
        {
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var singer = new Singers
                {
                    SingerId = 3,
                    FirstName = "Allis",
                    LastName = "Morrison",
                    BirthDate = new DateTime(1968, 5, 4),
                };
                db.Singers.Add(singer);

                var album = new Albums
                {
                    SingerId = singer.SingerId,
                    AlbumId = 2,
                    Title = "Allis Morrison's second album",
                    ReleaseDate = new DateTime(1987, 12, 24),
                };
                db.Albums.Add(album);

                var track = new Tracks
                {
                    AlbumId = 2,
                    TrackId = 1,
                    Title = "Track 1",
                    Duration = 4.32m,
                    Lyrics = new List<string> { "Song lyrics", "Liedtext" },
                    LyricsLanguages = new List<string> { "EN", "DE" },
                };
                db.Tracks.Add(track);
                await db.SaveChangesAsync();

                // Check that we can use the Album reference.
                Assert.Equal("Allis Morrison's second album", track.Album.Title);
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the track from the database.
                var track = await db.Tracks.FindAsync(2L, 1L);
                Assert.Equal("Track 1", track.Title);
                // Check that the link with Album works.
                Assert.NotNull(track.Album);
                Assert.Equal("Allis Morrison's second album", track.Album.Title);
            }
        }

        [Fact]
        public async void CanInsertConcertAndPerformances()
        {
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var singer = new Singers
                {
                    SingerId = 4,
                    FirstName = "Rob",
                    LastName = "Morrison",
                };
                db.Singers.Add(singer);

                var album = new Albums
                {
                    SingerId = singer.SingerId,
                    AlbumId = 4,
                    Title = "Rob Morrison's first album",
                };
                db.Albums.Add(album);

                var track = new Tracks
                {
                    AlbumId = album.AlbumId,
                    TrackId = 1,
                    Title = "Rob Morrison's first track",
                };
                db.Tracks.Add(track);

                var venue = new Venues
                {
                    Code = "PARK",
                    Name = "Central Park",
                };
                db.Venues.Add(venue);

                var concert = new Concerts
                {
                    VenueCode = "PARK",
                    StartTime = new DateTime(2020, 12, 28, 10, 0, 0),
                    SingerId = 4,
                    Title = "End of year concert",
                };
                db.Concerts.Add(concert);

                var performance = new Performances
                {
                    VenueCode = concert.VenueCode,
                    ConcertStartTime = concert.StartTime,
                    SingerId = concert.SingerId,
                    StartTime = concert.StartTime.AddHours(1),
                    AlbumId = track.AlbumId,
                    TrackId = track.TrackId,
                    Rating = 9.8D,
                };
                db.Performances.Add(performance);
                await db.SaveChangesAsync();
            }
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the concert from the database.
                var refreshedConcert = await db.Concerts.FindAsync("PARK", new DateTime(2020, 12, 28, 10, 0, 0), 4L);
                Assert.Equal("End of year concert", refreshedConcert.Title);
                // Check that the concert turns up in the collections of other entities.
                var singer = await db.Singers.FindAsync(4L);
                Assert.Collection(singer.Concerts, c => c.Equals(refreshedConcert));
                Assert.Equal(1, singer.Concerts.Count);
                var venue = await db.Venues.FindAsync("PARK");
                Assert.Collection(venue.Concerts, c => c.Equals(refreshedConcert));
                Assert.Equal(1, venue.Concerts.Count);

                // Check the track
                var track = await db.Tracks.FindAsync(4L, 1L);
                Assert.Equal("Rob Morrison's first track", track.Title);

                // Reget the performance from the database.
                var performance = await db.Performances.FindAsync(venue.Code, singer.SingerId, refreshedConcert.StartTime.AddHours(1));
                Assert.Equal(9.8D, performance.Rating);
                Assert.NotNull(performance.Tracks);
                Assert.NotNull(performance.Tracks.Album);
                Assert.Equal("Rob Morrison's first album", performance.Tracks.Album.Title);
            }
        }
    }
}
