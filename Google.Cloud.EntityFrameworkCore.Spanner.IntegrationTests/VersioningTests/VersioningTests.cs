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

using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.VersioningTests.Model;
using Microsoft.EntityFrameworkCore;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.VersioningTests
{
    public class VersioningTests : IClassFixture<VersioningTestFixture>
    {
        private readonly VersioningTestFixture _fixture;

        public VersioningTests(VersioningTestFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task InitialVersionIsGeneratedAtInsert()
        {
            var singerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            using (var db = new TestSpannerVersionDbContext(_fixture.DatabaseName))
            {
                db.Singers.Add(new SingersWithVersion
                {
                    SingerId = singerId,
                    FirstName = "Joe",
                    LastName = "Elliot",
                });
                db.Albums.Add(new AlbumsWithVersion
                {
                    AlbumId = albumId,
                    SingerId = singerId,
                    Title = "Some title",
                });
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerVersionDbContext(_fixture.DatabaseName))
            {
                var singer = await db.Singers.FindAsync(singerId);

                Assert.Equal(1, singer.Version);
                Assert.Collection(singer.Albums, a => Assert.Equal(1, a.Version));
            }
        }

        [Fact]
        public async Task VersionIsUpdated()
        {
            var singerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            using (var db = new TestSpannerVersionDbContext(_fixture.DatabaseName))
            {
                var singer = new SingersWithVersion
                {
                    SingerId = singerId,
                    FirstName = "Pete",
                    LastName = "Allison",
                };
                var album = new AlbumsWithVersion
                {
                    AlbumId = albumId,
                    SingerId = singerId,
                    Title = "A new title",
                };
                db.AddRange(singer, album);
                await db.SaveChangesAsync();

                // Update both the singer and album.
                singer.FirstName = "Zeke";
                album.Title = "Other title";
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerVersionDbContext(_fixture.DatabaseName))
            {
                var singer = await db.Singers.FindAsync(singerId);

                Assert.Equal(2, singer.Version);
                Assert.Collection(singer.Albums, a => Assert.Equal(2, a.Version));
            }
        }

        [Fact]
        public async Task UpdateIsRejectedIfConcurrentUpdateIsDetected()
        {
            var singerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            using var db = new TestSpannerVersionDbContext(_fixture.DatabaseName);
            var singer = new SingersWithVersion
            {
                SingerId = singerId,
                FirstName = "Pete",
                LastName = "Allison",
            };
            var album = new AlbumsWithVersion
            {
                AlbumId = albumId,
                SingerId = singerId,
                Title = "A new title",
            };
            db.AddRange(singer, album);
            await db.SaveChangesAsync();

            // Update the version number of the records manually to simulate a concurrent update.
            Assert.Equal(1, await db.Database.ExecuteSqlInterpolatedAsync($"UPDATE SingersWithVersion SET Version=2 WHERE SingerId={singerId}"));
            Assert.Equal(1, await db.Database.ExecuteSqlInterpolatedAsync($"UPDATE AlbumsWithVersion SET Version=2 WHERE SingerId={singerId} AND AlbumId={albumId}"));

            // Try to update the singer and album through EF Core. That should now fail.
            singer.FirstName = "Zeke";
            album.Title = "Other title";
            await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync());
        }

        [Fact]
        public async Task CanDeleteWithCascade()
        {
            var singerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            using var db = new TestSpannerVersionDbContext(_fixture.DatabaseName);
            var singer = new SingersWithVersion
            {
                SingerId = singerId,
                FirstName = "Pete",
                LastName = "Allison",
            };
            var album = new AlbumsWithVersion
            {
                AlbumId = albumId,
                SingerId = singerId,
                Title = "A new title",
            };
            db.AddRange(singer, album);
            Assert.Equal(2, await db.SaveChangesAsync());

            // Now delete the singer record. This will also cascade delete the album.
            // The total record count will therefore be 2.
            db.Singers.Remove(singer);
            Assert.Equal(2, await db.SaveChangesAsync());
        }

        [Fact]
        public async Task DeleteIsRejectedIfConcurrentUpdateIsDetected()
        {
            var singerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            using var db = new TestSpannerVersionDbContext(_fixture.DatabaseName);
            var singer = new SingersWithVersion
            {
                SingerId = singerId,
                FirstName = "Pete",
                LastName = "Allison",
            };
            var album = new AlbumsWithVersion
            {
                AlbumId = albumId,
                SingerId = singerId,
                Title = "A new title",
            };
            db.AddRange(singer, album);
            Assert.Equal(2, await db.SaveChangesAsync());

            // Update the version number of the records manually to simulate a concurrent update.
            Assert.Equal(1, await db.Database.ExecuteSqlInterpolatedAsync($"UPDATE SingersWithVersion SET Version=2 WHERE SingerId={singerId}"));
            Assert.Equal(1, await db.Database.ExecuteSqlInterpolatedAsync($"UPDATE AlbumsWithVersion SET Version=2 WHERE SingerId={singerId} AND AlbumId={albumId}"));

            // Try to delete the singer. This will fail as the version number no longer matches.
            db.Singers.Remove(singer);
            await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => db.SaveChangesAsync());
        }
    }
}
