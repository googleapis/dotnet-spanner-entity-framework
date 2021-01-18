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
using System.Text;
using System.Linq;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    public class ScaffoldingTests : IClassFixture<SpannerSampleFixture>
    {
        private readonly SpannerSampleFixture _fixture;

        public ScaffoldingTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [Fact]
        public async void AllTablesAreGenerated()
        {
            using var connection = _fixture.GetConnection();
            var tableNames = new string[] {
                "Singers", "Albums", "Tracks", "Venues", "Concerts", "Performances", "TableWithAllColumnTypes"
            };
            var tables = new SpannerParameterCollection
            {
                { "tables", SpannerDbType.ArrayOf(SpannerDbType.String), tableNames }
            };
            var cmd = connection.CreateSelectCommand(
                "SELECT COUNT(*) " +
                "FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_CATALOG='' AND TABLE_SCHEMA='' AND TABLE_NAME IN UNNEST (@tables)", tables);
            using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(tableNames.Length, reader.GetInt64(0));
            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async void CanInsertAndUpdateVenue()
        {
            var code = _fixture.RandomString(4);
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var venue = new Venues
                {
                    Code = code,
                    Name = "Concert Hall",
                    Active = true,
                    Capacity = 2000,
                    Ratings = new List<double?> { 8.9, 6.5, 8.0 },
                };
                db.Venues.Add(venue);
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the venue from the database.
                var venue = await db.Venues.FindAsync(code);
                Assert.Equal("Concert Hall", venue.Name);
                Assert.Equal(2000, venue.Capacity);
                Assert.Equal(new List<double?> { 8.9, 6.5, 8.0 }, venue.Ratings);

                // Update the venue.
                venue.Name = "Concert Hall - Refurbished";
                venue.Capacity = 3000;
                // TODO: Preferably it should be possible to just call List.AddRange(...) to
                // update a list.
                venue.Ratings = new List<double?>(venue.Ratings.Union(new double?[] { 9.5, 9.8, 10.0 }));
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the venue from the database.
                var venue = await db.Venues.FindAsync(code);
                Assert.Equal("Concert Hall - Refurbished", venue.Name);
                Assert.Equal(3000, venue.Capacity);
                Assert.Equal(new List<double?> { 8.9, 6.5, 8.0, 9.5, 9.8, 10.0 }, venue.Ratings);
            }
        }

        [Fact]
        public async void CanInsertAndUpdateSinger()
        {
            var singerId = _fixture.RandomLong();
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var singer = new Singers
                {
                    SingerId = singerId,
                    FirstName = "Rob",
                    LastName = "Morrison",
                    BirthDate = new SpannerDate(2002, 10, 1),
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
                var singer = await db.Singers.FindAsync(singerId);
                Assert.Equal("Rob", singer.FirstName);
                Assert.Equal("Morrison", singer.LastName);
                Assert.Equal(new SpannerDate(2002, 10, 1), singer.BirthDate);
                Assert.Equal(new byte[] { 1, 2, 3 }, singer.Picture);
                Assert.Equal("Rob Morrison", singer.FullName);

                // Update the singer.
                singer.FirstName = "Alice";
                singer.LastName = "Morrison - Chine";
                singer.BirthDate = new SpannerDate(2002, 10, 15);
                singer.Picture = new byte[] { 3, 2, 1 };
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the singer from the database.
                var singer = await db.Singers.FindAsync(singerId);
                Assert.Equal("Alice", singer.FirstName);
                Assert.Equal("Morrison - Chine", singer.LastName);
                Assert.Equal(new SpannerDate(2002, 10, 15), singer.BirthDate);
                Assert.Equal(new byte[] { 3, 2, 1 }, singer.Picture);
                Assert.Equal("Alice Morrison - Chine", singer.FullName);
            }
        }

        [Fact]
        public async void CanInsertAndUpdateAlbum()
        {
            var singerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var singer = new Singers
                {
                    SingerId = singerId,
                    FirstName = "Pete",
                    LastName = "Henderson",
                    BirthDate = new SpannerDate(1997, 2, 20),
                };
                db.Singers.Add(singer);
                var album = new Albums
                {
                    SingerId = singer.SingerId,
                    AlbumId = albumId,
                    Title = "Pete Henderson's first album",
                    ReleaseDate = new SpannerDate(2019, 04, 19),
                };
                db.Albums.Add(album);
                await db.SaveChangesAsync();

                // Check that we can use the Singer reference.
                Assert.Equal("Pete Henderson", album.Singer.FullName);
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the album from the database.
                var album = await db.Albums.FindAsync(albumId);
                Assert.Equal("Pete Henderson's first album", album.Title);
                Assert.Equal(new SpannerDate(2019, 4, 19), album.ReleaseDate);
                Assert.Equal(singerId, album.SingerId);
                Assert.NotNull(album.Singer);

                // Update the album.
                album.Title = "Pete Henderson's first album - Refurbished";
                album.ReleaseDate = new SpannerDate(2020, 2, 29);
                await db.SaveChangesAsync();
            }

            var newSingerId = _fixture.RandomLong();
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the album from the database.
                var album = await db.Albums.FindAsync(albumId);
                Assert.Equal("Pete Henderson's first album - Refurbished", album.Title);
                Assert.Equal(new SpannerDate(2020, 2, 29), album.ReleaseDate);

                // Insert another singer and update the album to that singer.
                var singer = new Singers
                {
                    SingerId = newSingerId,
                    FirstName = "Alice",
                    LastName = "Robertson",
                };
                db.Singers.Add(singer);
                album.Singer = singer;
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the album from the database and check that the singer was updated.
                var album = await db.Albums.FindAsync(albumId);
                Assert.Equal(newSingerId, album.SingerId);
                Assert.Equal("Alice Robertson", album.Singer.FullName);
            }
        }

        [Fact]
        public async void CanInsertAndUpdateTrack()
        {
            var singerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            var trackId = _fixture.RandomLong();
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var singer = new Singers
                {
                    SingerId = singerId,
                    FirstName = "Allis",
                    LastName = "Morrison",
                    BirthDate = new SpannerDate(1968, 5, 4),
                };
                db.Singers.Add(singer);

                var album = new Albums
                {
                    SingerId = singer.SingerId,
                    AlbumId = albumId,
                    Title = "Allis Morrison's second album",
                    ReleaseDate = new SpannerDate(1987, 12, 24),
                };
                db.Albums.Add(album);

                var track = new Tracks
                {
                    AlbumId = albumId,
                    TrackId = trackId,
                    Title = "Track 1",
                    Duration = (SpannerNumeric?)4.32m,
                    Lyrics = new List<string> { "Song lyrics", "Liedtext" },
                    LyricsLanguages = new List<string> { "EN", "DE" },
                };
                db.Tracks.Add(track);
                await db.SaveChangesAsync();

                // Check that we can use the album reference.
                Assert.Equal("Allis Morrison's second album", track.Album.Title);
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the track from the database.
                var track = await db.Tracks.FindAsync(albumId, trackId);
                Assert.Equal("Track 1", track.Title);
                Assert.Equal((SpannerNumeric?)4.32m, track.Duration);
                Assert.Equal(new List<string> { "Song lyrics", "Liedtext" }, track.Lyrics);
                Assert.Equal(new List<string> { "EN", "DE" }, track.LyricsLanguages);
                // Check that the link with album works.
                Assert.NotNull(track.Album);
                Assert.Equal("Allis Morrison's second album", track.Album.Title);

                // Update the track.
                track.Title = "Track 1 - Refurbished";
                track.Duration = (SpannerNumeric?)4.35m;
                track.Lyrics = new List<string>(track.Lyrics.Union(new string[] { "Sangtekst" }));
                track.LyricsLanguages = new List<string>(track.LyricsLanguages.Union(new string[] { "NO" }));
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the track from the database.
                var track = await db.Tracks.FindAsync(albumId, trackId);
                Assert.Equal("Track 1 - Refurbished", track.Title);
                Assert.Equal((SpannerNumeric?)4.35m, track.Duration);
                Assert.Equal(new List<string> { "Song lyrics", "Liedtext", "Sangtekst" }, track.Lyrics);
                Assert.Equal(new List<string> { "EN", "DE", "NO" }, track.LyricsLanguages);
            }
        }

        [Fact]
        public async void CanInsertAndUpdateConcertsAndPerformances()
        {
            var singerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            var trackId = _fixture.RandomLong();
            var venueCode = _fixture.RandomString(4);
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var singer = new Singers
                {
                    SingerId = singerId,
                    FirstName = "Rob",
                    LastName = "Morrison",
                };
                db.Singers.Add(singer);

                var album = new Albums
                {
                    SingerId = singer.SingerId,
                    AlbumId = albumId,
                    Title = "Rob Morrison's first album",
                };
                db.Albums.Add(album);

                var track = new Tracks
                {
                    AlbumId = album.AlbumId,
                    TrackId = trackId,
                    Title = "Rob Morrison's first track",
                };
                db.Tracks.Add(track);

                var venue = new Venues
                {
                    Code = venueCode,
                    Name = "Central Park",
                };
                db.Venues.Add(venue);

                var concert = new Concerts
                {
                    VenueCode = venueCode,
                    StartTime = new DateTime(2020, 12, 28, 10, 0, 0),
                    SingerId = singerId,
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
                var concert = await db.Concerts.FindAsync(venueCode, new DateTime(2020, 12, 28, 10, 0, 0), singerId);
                Assert.Equal("End of year concert", concert.Title);
                Assert.Equal(singerId, concert.SingerId);
                // Check that the concert turns up in the collections of other entities.
                var singer = await db.Singers.FindAsync(singerId);
                Assert.Collection(singer.Concerts, c => c.Equals(concert));
                Assert.Equal(1, singer.Concerts.Count);
                var venue = await db.Venues.FindAsync(venueCode);
                Assert.Collection(venue.Concerts, c => c.Equals(concert));
                Assert.Equal(1, venue.Concerts.Count);

                // Check the track
                var track = await db.Tracks.FindAsync(albumId, trackId);
                Assert.Equal("Rob Morrison's first track", track.Title);

                // Reget the performance from the database.
                var performance = await db.Performances.FindAsync(venue.Code, singer.SingerId, concert.StartTime.AddHours(1));
                Assert.Equal(9.8D, performance.Rating);
                Assert.NotNull(performance.Tracks);
                Assert.NotNull(performance.Tracks.Album);
                Assert.Equal("Rob Morrison's first album", performance.Tracks.Album.Title);

                // Update the concert.
                concert.Title = "End of year concert - Postponed until next year";
                // Update the performance.
                performance.Rating = 8.9D;
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var concert = await db.Concerts.FindAsync(venueCode, new DateTime(2020, 12, 28, 10, 0, 0), singerId);
                Assert.Equal("End of year concert - Postponed until next year", concert.Title);
                var performance = await db.Performances.FindAsync(concert.VenueCode, concert.SingerId, concert.StartTime.AddHours(1));
                Assert.Equal(8.9D, performance.Rating);
            }
        }

        [Fact]
        public async void CanInsertAndUpdateRowWithAllDataTypes()
        {
            var id = _fixture.RandomLong();
            var today = SpannerDate.FromDateTime(DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Unspecified));
            var now = DateTime.UtcNow;
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var row = new TableWithAllColumnTypes
                {
                    ColBool = true,
                    ColBoolArray = new List<bool?> { true, false, true },
                    ColBytes = new byte[] { 1, 2, 3 },
                    ColBytesMax = Encoding.UTF8.GetBytes("This is a long string"),
                    ColBytesArray = new List<byte[]> { new byte[] { 3, 2, 1 }, new byte[] { }, new byte[] { 4, 5, 6 } },
                    ColBytesMaxArray = new List<byte[]> { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2"), Encoding.UTF8.GetBytes("string 3") },
                    ColDate = new SpannerDate(2020, 12, 28),
                    ColDateArray = new List<SpannerDate?> { new SpannerDate(2020, 12, 28), new SpannerDate(2010, 1, 1), today },
                    ColFloat64 = 3.14D,
                    ColFloat64Array = new List<double?> { 3.14D, 6.626D },
                    ColInt64 = id,
                    ColInt64Array = new List<long?> { 1L, 2L, 4L, 8L },
                    ColNumeric = (SpannerNumeric?)3.14m,
                    ColNumericArray = new List<SpannerNumeric?> { (SpannerNumeric)3.14m, (SpannerNumeric)6.626m },
                    ColString = "some string",
                    ColStringArray = new List<string> { "string1", "string2", "string3" },
                    ColStringMax = "some longer string",
                    ColStringMaxArray = new List<string> { "longer string1", "longer string2", "longer string3" },
                    ColTimestamp = new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288),
                    ColTimestampArray = new List<DateTime?> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now },
                };
                db.TableWithAllColumnTypes.Add(row);
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the row from the database.
                var row = await db.TableWithAllColumnTypes.FindAsync(id);
                Assert.True(row.ColBool);
                Assert.Equal(new List<bool?> { true, false, true }, row.ColBoolArray);
                Assert.Equal(new byte[] { 1, 2, 3 }, row.ColBytes);
                Assert.Equal(Encoding.UTF8.GetBytes("This is a long string"), row.ColBytesMax);
                Assert.Equal(new List<byte[]> { new byte[] { 3, 2, 1 }, new byte[] { }, new byte[] { 4, 5, 6 } }, row.ColBytesArray);
                Assert.Equal(new List<byte[]> { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2"), Encoding.UTF8.GetBytes("string 3") }, row.ColBytesMaxArray);
                Assert.Equal(new SpannerDate(2020, 12, 28), row.ColDate);
                Assert.Equal(new List<SpannerDate?> { new SpannerDate(2020, 12, 28), new SpannerDate(2010, 1, 1), today }, row.ColDateArray);
                Assert.Equal(3.14D, row.ColFloat64);
                Assert.Equal(new List<double?> { 3.14D, 6.626D }, row.ColFloat64Array);
                Assert.Equal((SpannerNumeric?)3.14m, row.ColNumeric);
                Assert.Equal(new List<SpannerNumeric?> { (SpannerNumeric)3.14m, (SpannerNumeric)6.626m }, row.ColNumericArray);
                Assert.Equal(id, row.ColInt64);
                Assert.Equal(new List<long?> { 1L, 2L, 4L, 8L }, row.ColInt64Array);
                Assert.Equal("some string", row.ColString);
                Assert.Equal(new List<string> { "string1", "string2", "string3" }, row.ColStringArray);
                Assert.Equal("some longer string", row.ColStringMax);
                Assert.Equal(new List<string> { "longer string1", "longer string2", "longer string3" }, row.ColStringMaxArray);
                Assert.Equal(new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), row.ColTimestamp);
                Assert.Equal(new List<DateTime?> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now }, row.ColTimestampArray);

                // The commit timestamp was automatically set by Cloud Spanner.
                Assert.NotEqual(new DateTime(), row.ColCommitTs);
                // This assumes that the local time does not differ more than 10 minutes with TrueTime.
                Assert.True(Math.Abs(DateTime.UtcNow.Subtract(row.ColCommitTs.GetValueOrDefault()).TotalMinutes) < 10, $"Commit timestamp {row.ColCommitTs} differs with more than 10 minutes from now ({DateTime.UtcNow})");

                // Update the row.
                row.ColBool = false;
                row.ColBoolArray = new List<bool?> { false, true, false };
                row.ColBytes = new byte[] { 3, 2, 1 };
                row.ColBytesMax = Encoding.UTF8.GetBytes("This string has changed");
                row.ColBytesArray = new List<byte[]> { new byte[] { 10, 20, 30 }, new byte[] { 40, 50, 60 } };
                row.ColBytesMaxArray = new List<byte[]> { Encoding.UTF8.GetBytes("changed string 1"), Encoding.UTF8.GetBytes("changed string 2"), Encoding.UTF8.GetBytes("changed string 3") };
                row.ColDate = new SpannerDate(2020, 12, 30);
                row.ColDateArray = new List<SpannerDate?> { today, new SpannerDate(2020, 12, 30), new SpannerDate(2010, 2, 28) };
                row.ColFloat64 = 1.234D;
                row.ColFloat64Array = new List<double?> { 1.0D, 1.1D, 1.11D };
                row.ColNumeric = (SpannerNumeric?)1.234m;
                row.ColNumericArray = new List<SpannerNumeric?> { (SpannerNumeric)1.0m, (SpannerNumeric)1.1m, (SpannerNumeric)1.11m };
                row.ColInt64Array = new List<long?> { 500L, 1000L };
                row.ColString = "some changed string";
                row.ColStringArray = new List<string> { "changed string1", "changed string2", "changed string3" };
                row.ColStringMax = "some longer changed string";
                row.ColStringMaxArray = new List<string> { "changed longer string1", "changed longer string2", "changed longer string3" };
                row.ColTimestamp = new DateTime(2020, 12, 30, 15, 16, 28, 148).AddTicks(5498);
                row.ColTimestampArray = new List<DateTime?> { now, new DateTime(2020, 12, 30, 15, 16, 28, 148).AddTicks(5498) };
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Reget the row from the database.
                var row = await db.TableWithAllColumnTypes.FindAsync(id);
                Assert.False(row.ColBool);
                Assert.Equal(new List<bool?> { false, true, false }, row.ColBoolArray);
                Assert.Equal(new byte[] { 3, 2, 1 }, row.ColBytes);
                Assert.Equal(Encoding.UTF8.GetBytes("This string has changed"), row.ColBytesMax);
                Assert.Equal(new List<byte[]> { new byte[] { 10, 20, 30 }, new byte[] { 40, 50, 60 } }, row.ColBytesArray);
                Assert.Equal(new List<byte[]> { Encoding.UTF8.GetBytes("changed string 1"), Encoding.UTF8.GetBytes("changed string 2"), Encoding.UTF8.GetBytes("changed string 3") }, row.ColBytesMaxArray);
                Assert.Equal(new SpannerDate(2020, 12, 30), row.ColDate);
                Assert.Equal(new List<SpannerDate?> { today, new SpannerDate(2020, 12, 30), new SpannerDate(2010, 2, 28) }, row.ColDateArray);
                Assert.Equal(1.234D, row.ColFloat64);
                Assert.Equal(new List<double?> { 1.0D, 1.1D, 1.11D }, row.ColFloat64Array);
                Assert.Equal((SpannerNumeric?)1.234m, row.ColNumeric);
                Assert.Equal(new List<SpannerNumeric?> { (SpannerNumeric)1.0m, (SpannerNumeric)1.1m, (SpannerNumeric)1.11m }, row.ColNumericArray);
                Assert.Equal(new List<long?> { 500L, 1000L }, row.ColInt64Array);
                Assert.Equal("some changed string", row.ColString);
                Assert.Equal(new List<string> { "changed string1", "changed string2", "changed string3" }, row.ColStringArray);
                Assert.Equal("some longer changed string", row.ColStringMax);
                Assert.Equal(new List<string> { "changed longer string1", "changed longer string2", "changed longer string3" }, row.ColStringMaxArray);
                Assert.Equal(new DateTime(2020, 12, 30, 15, 16, 28, 148).AddTicks(5498), row.ColTimestamp);
                Assert.Equal(new List<DateTime?> { now, new DateTime(2020, 12, 30, 15, 16, 28, 148).AddTicks(5498) }, row.ColTimestampArray);
            }
        }

        [Fact]
        public async void CanInsertAndUpdateNullValues()
        {
            var id = _fixture.RandomLong();
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Create a row with all null values except for the primary key.
                // Cloud Spanner does support rows with a null value for the PK,
                // but EFCore does not support that.
                var row = new TableWithAllColumnTypes { ColInt64 = id };
                db.TableWithAllColumnTypes.Add(row);
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var row = await db.TableWithAllColumnTypes.FindAsync(id);
                Assert.Null(row.ColBool);
                Assert.Null(row.ColBoolArray);
                Assert.Null(row.ColBytes);
                Assert.Null(row.ColBytesArray);
                Assert.Null(row.ColBytesMax);
                Assert.Null(row.ColBytesMaxArray);
                Assert.NotNull(row.ColCommitTs); // Automatically filled on commit.
                Assert.Null(row.ColComputed);
                Assert.Null(row.ColDate);
                Assert.Null(row.ColDateArray);
                Assert.Null(row.ColFloat64);
                Assert.Null(row.ColFloat64Array);
                Assert.Null(row.ColNumeric);
                Assert.Null(row.ColNumericArray);
                Assert.Null(row.ColInt64Array);
                Assert.Null(row.ColString);
                Assert.Null(row.ColStringArray);
                Assert.Null(row.ColStringMax);
                Assert.Null(row.ColStringMaxArray);
                Assert.Null(row.ColTimestamp);
                Assert.Null(row.ColTimestampArray);

                // Update from null to non-null.
                row.ColBool = true;
                row.ColBoolArray = new List<bool?> { };
                row.ColBytes = new byte[0];
                row.ColBytesArray = new List<byte[]> { };
                row.ColBytesMax = new byte[0];
                row.ColBytesMaxArray = new List<byte[]> { };
                row.ColDate = new SpannerDate(1, 1, 1);
                row.ColDateArray = new List<SpannerDate?> { };
                row.ColFloat64 = 0.0D;
                row.ColFloat64Array = new List<double?> { };
                row.ColNumeric = (SpannerNumeric?)0.0m;
                row.ColNumericArray = new List<SpannerNumeric?> { };
                row.ColInt64Array = new List<long?> { };
                row.ColString = "";
                row.ColStringArray = new List<string> { };
                row.ColStringMax = "";
                row.ColStringMaxArray = new List<string> { };
                row.ColTimestamp = new DateTime(1, 1, 1, 0, 0, 0);
                row.ColTimestampArray = new List<DateTime?> { };
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var row = await db.TableWithAllColumnTypes.FindAsync(id);
                Assert.NotNull(row.ColBool);
                Assert.NotNull(row.ColBoolArray);
                Assert.NotNull(row.ColBytes);
                Assert.NotNull(row.ColBytesArray);
                Assert.NotNull(row.ColBytesMax);
                Assert.NotNull(row.ColBytesMaxArray);
                Assert.NotNull(row.ColCommitTs);
                Assert.NotNull(row.ColComputed);
                Assert.NotNull(row.ColDate);
                Assert.NotNull(row.ColDateArray);
                Assert.NotNull(row.ColFloat64);
                Assert.NotNull(row.ColFloat64Array);
                Assert.NotNull(row.ColNumeric);
                Assert.NotNull(row.ColNumericArray);
                Assert.NotNull(row.ColInt64Array);
                Assert.NotNull(row.ColString);
                Assert.NotNull(row.ColStringArray);
                Assert.NotNull(row.ColStringMax);
                Assert.NotNull(row.ColStringMaxArray);
                Assert.NotNull(row.ColTimestamp);
                Assert.NotNull(row.ColTimestampArray);

                // Update from non-null back to null.
                row.ColBool = null;
                row.ColBoolArray = null;
                row.ColBytes = null;
                row.ColBytesArray = null;
                row.ColBytesMax = null;
                row.ColBytesMaxArray = null;
                row.ColDate = null;
                row.ColDateArray = null;
                row.ColFloat64 = null;
                row.ColFloat64Array = null;
                row.ColNumeric = null;
                row.ColNumericArray = null;
                row.ColInt64Array = null;
                row.ColString = null;
                row.ColStringArray = null;
                row.ColStringMax = null;
                row.ColStringMaxArray = null;
                row.ColTimestamp = null;
                row.ColTimestampArray = null;
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var row = await db.TableWithAllColumnTypes.FindAsync(id);
                Assert.Null(row.ColBool);
                Assert.Null(row.ColBoolArray);
                Assert.Null(row.ColBytes);
                Assert.Null(row.ColBytesArray);
                Assert.Null(row.ColBytesMax);
                Assert.Null(row.ColBytesMaxArray);
                Assert.NotNull(row.ColCommitTs); // Automatically filled on commit.
                Assert.Null(row.ColComputed);
                Assert.Null(row.ColDate);
                Assert.Null(row.ColDateArray);
                Assert.Null(row.ColFloat64);
                Assert.Null(row.ColFloat64Array);
                Assert.Null(row.ColNumeric);
                Assert.Null(row.ColNumericArray);
                Assert.Null(row.ColInt64Array);
                Assert.Null(row.ColString);
                Assert.Null(row.ColStringArray);
                Assert.Null(row.ColStringMax);
                Assert.Null(row.ColStringMaxArray);
                Assert.Null(row.ColTimestamp);
                Assert.Null(row.ColTimestampArray);
            }
        }

        [Fact]
        public async void CanInsertAndUpdateNullValuesInArrays()
        {
            var id = _fixture.RandomLong();
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var row = new TableWithAllColumnTypes
                {
                    ColInt64 = id,
                    ColBoolArray = new List<bool?> { true, null, false },
                    ColBytesArray = new List<byte[]> { new byte[] { 1 }, null, new byte[] { 2 } },
                    ColBytesMaxArray = new List<byte[]> { new byte[] { 1 }, null, new byte[] { 2 } },
                    ColDateArray = new List<SpannerDate?> { new SpannerDate(2020, 1, 13), null, new SpannerDate(2021, 1, 13) },
                    ColFloat64Array = new List<double?> { 3.14, null, 6.662 },
                    ColInt64Array = new List<long?> { 100, null, 200 },
                    ColNumericArray = new List<SpannerNumeric?> { (SpannerNumeric)3.14m, null, (SpannerNumeric)6.662m },
                    ColStringArray = new List<string> { "string1", null, "string2" },
                    ColStringMaxArray = new List<string> { "long string 1", null, "long string 2" },
                    ColTimestampArray = new List<DateTime?> { new DateTime(2021, 1, 13, 15, 24, 19), null },
                };

                db.TableWithAllColumnTypes.Add(row);
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var row = await db.TableWithAllColumnTypes.FindAsync(id);
                Assert.Equal(new List<bool?> { true, null, false }, row.ColBoolArray);
                Assert.Equal(new List<byte[]> { new byte[] { 1 }, null, new byte[] { 2 } }, row.ColBytesArray);
                Assert.Equal(new List<byte[]> { new byte[] { 1 }, null, new byte[] { 2 } }, row.ColBytesMaxArray);
                Assert.Equal(new List<SpannerDate?> { new SpannerDate(2020, 1, 13), null, new SpannerDate(2021, 1, 13) }, row.ColDateArray);
                Assert.Equal(new List<double?> { 3.14, null, 6.662 }, row.ColFloat64Array);
                Assert.Equal(new List<long?> { 100, null, 200 }, row.ColInt64Array);
                Assert.Equal(new List<SpannerNumeric?> { (SpannerNumeric)3.14m, null, (SpannerNumeric)6.662m }, row.ColNumericArray);
                Assert.Equal(new List<string> { "string1", null, "string2" }, row.ColStringArray);
                Assert.Equal(new List<string> { "long string 1", null, "long string 2" }, row.ColStringMaxArray);
                Assert.Equal(new List<DateTime?> { new DateTime(2021, 1, 13, 15, 24, 19), null }, row.ColTimestampArray);

                row.ColBoolArray = new List<bool?> { null, true, null };
                row.ColBytesArray = new List<byte[]> { new byte[] { 1 }, null, new byte[] { 2 } };
                row.ColBytesMaxArray = new List<byte[]> { new byte[] { 1 }, null, new byte[] { 2 } };
                row.ColDateArray = new List<SpannerDate?> { new SpannerDate(2020, 1, 13), null, new SpannerDate(2021, 1, 13) };
                row.ColFloat64Array = new List<double?> { 3.14, null, 6.662 };
                row.ColInt64Array = new List<long?> { 100, null, 200 };
                row.ColNumericArray = new List<SpannerNumeric?> { (SpannerNumeric)3.14m, null, (SpannerNumeric)6.662m };
                row.ColStringArray = new List<string> { "string1", null, "string2" };
                row.ColStringMaxArray = new List<string> { "long string 1", null, "long string 2" };
                row.ColTimestampArray = new List<DateTime?> { new DateTime(2021, 1, 13, 15, 24, 19), null };
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                var row = await db.TableWithAllColumnTypes.FindAsync(id);
                Assert.Equal(new List<bool?> { null, true, null }, row.ColBoolArray);
                Assert.Equal(new List<byte[]> { new byte[] { 1 }, null, new byte[] { 2 } }, row.ColBytesArray);
                Assert.Equal(new List<byte[]> { new byte[] { 1 }, null, new byte[] { 2 } }, row.ColBytesMaxArray);
                Assert.Equal(new List<SpannerDate?> { new SpannerDate(2020, 1, 13), null, new SpannerDate(2021, 1, 13) }, row.ColDateArray);
                Assert.Equal(new List<double?> { 3.14, null, 6.662 }, row.ColFloat64Array);
                Assert.Equal(new List<long?> { 100, null, 200 }, row.ColInt64Array);
                Assert.Equal(new List<SpannerNumeric?> { (SpannerNumeric)3.14m, null, (SpannerNumeric)6.662m }, row.ColNumericArray);
                Assert.Equal(new List<string> { "string1", null, "string2" }, row.ColStringArray);
                Assert.Equal(new List<string> { "long string 1", null, "long string 2" }, row.ColStringMaxArray);
                Assert.Equal(new List<DateTime?> { new DateTime(2021, 1, 13, 15, 24, 19), null }, row.ColTimestampArray);
            }
        }

        [Fact]
        public async void CanDeleteData()
        {
            var singerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            var trackId = _fixture.RandomLong();
            var venueCode = _fixture.RandomString(4);
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singer = new Singers
            {
                SingerId = singerId,
                LastName = "To be deleted",
            };
            db.Singers.Add(singer);

            var album = new Albums
            {
                AlbumId = albumId,
                Title = "To be deleted",
                SingerId = singer.SingerId,
            };
            db.Albums.Add(album);

            var track = new Tracks
            {
                AlbumId = album.AlbumId,
                TrackId = trackId,
                Title = "To be deleted",
            };
            db.Tracks.Add(track);

            var venue = new Venues
            {
                Code = venueCode,
                Name = "To be deleted",
            };
            db.Venues.Add(venue);

            var concert = new Concerts
            {
                VenueCode = venue.Code,
                StartTime = new DateTime(2020, 12, 30, 10, 00, 30),
                SingerId = singer.SingerId,
            };
            db.Concerts.Add(concert);

            var performance = new Performances
            {
                VenueCode = venue.Code,
                ConcertStartTime = concert.StartTime,
                SingerId = singer.SingerId,
                AlbumId = album.AlbumId,
                TrackId = track.TrackId,
                StartTime = concert.StartTime.AddMinutes(30),
            };
            db.Performances.Add(performance);

            var id = _fixture.RandomLong();
            var row = new TableWithAllColumnTypes { ColInt64 = id };
            db.TableWithAllColumnTypes.Add(row);

            await db.SaveChangesAsync();

            // Delete all rows.
            db.TableWithAllColumnTypes.Remove(row);
            db.Performances.Remove(performance);
            db.Concerts.Remove(concert);
            db.Venues.Remove(venue);
            db.Tracks.Remove(track);
            db.Albums.Remove(album);
            db.Singers.Remove(singer);
            await db.SaveChangesAsync();

            // Verify that all rows were deleted.
            Assert.Null(await db.Singers.FindAsync(singer.SingerId));
            Assert.Null(await db.Albums.FindAsync(album.AlbumId));
            Assert.Null(await db.Tracks.FindAsync(album.AlbumId, track.TrackId));
            Assert.Null(await db.Venues.FindAsync(venue.Code));
            Assert.Null(await db.Concerts.FindAsync(concert.VenueCode, concert.StartTime, concert.SingerId));
            Assert.Null(await db.Performances.FindAsync(performance.VenueCode, performance.SingerId, performance.StartTime));
            Assert.Null(await db.TableWithAllColumnTypes.FindAsync(row.ColInt64));
        }
    }
}
