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

using Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel;
using System;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.Snippets
{
    /// <summary>
    /// Cloud Spanner supports writing the commit timestamp of a row: https://cloud.google.com/spanner/docs/commit-timestamp
    /// 
    /// This feature has one very important limitation that should be taken into consideration before choosing
    /// to use it: When writing a PENDING_COMMIT_TIMESTAMP to a table, that table will be unreadable for the
    /// remainder of the transaction. This means that you cannot execute any queries on the same table after
    /// calling SaveChanges during the same transaction. This is not an issue if you do not use manual transactions.
    /// 
    /// It also means that the commit timestamp value is not readable in the same database context as the one that
    /// updated it.
    /// 
    /// See also https://cloud.google.com/spanner/docs/commit-timestamp#dml
    /// </summary>
    public static class CommitTimestampSample
    {
        public static async Task Run(string connectionString)
        {
            Concert concert = null;
            Track track = null;
            var startTime = new DateTime(2021, 1, 27, 19, 0, 0, DateTimeKind.Utc);
            using (var context = new SpannerSampleDbContext(connectionString))
            {
                (concert, track) = await GetConcertAndTrackAsync(context);

                // Create a new performance and save it.
                // This will automatically fill the CreatedAt property with the commit timestamp of the transaction.
                var performance = new Performance
                {
                    VenueCode = concert.VenueCode,
                    SingerId = concert.SingerId,
                    ConcertStartTime = concert.StartTime,
                    AlbumId = track.AlbumId,
                    TrackId = track.TrackId,
                    StartTime = startTime,
                    Rating = 7.5,
                };
                context.Performances.Add(performance);
                var count = await context.SaveChangesAsync();
                Console.WriteLine($"Saved {count} performance");
            }

            // A generated commit timestamp is not readable within the same database context.
            // We there need to create a new context to read it back.
            using (var context = new SpannerSampleDbContext(connectionString))
            {
                // Read the performance from the database and check the CreatedAt value.
                var performance = await context.Performances.FindAsync(concert.VenueCode, concert.SingerId, startTime);
                Console.WriteLine($"Performance was created at {performance.CreatedAt}");

                var lastUpdated = performance.LastUpdatedAt == null ? "<never>" : performance.LastUpdatedAt.ToString();
                Console.WriteLine($"Performance was last updated at {lastUpdated}");

                // Update the performance. This will also fill the LastUpdatedAt property.
                performance.Rating = 8.5;
                var count = await context.SaveChangesAsync();
                Console.WriteLine($"Updated {count} performance");
            }

            using (var context = new SpannerSampleDbContext(connectionString))
            {
                var performance = await context.Performances.FindAsync(concert.VenueCode, concert.SingerId, startTime);
                Console.WriteLine($"Performance was created at {performance.CreatedAt}");
                Console.WriteLine($"Performance was updated at {performance.LastUpdatedAt}");
            }
        }

        private static async Task<(Concert, Track)> GetConcertAndTrackAsync(SpannerSampleDbContext context)
        {
            var singer = new Singer
            {
                SingerId = Guid.NewGuid(),
                FirstName = "Alice",
                LastName = "Jameson",
            };
            context.Singers.Add(singer);
            var album = new Album
            {
                AlbumId = Guid.NewGuid(),
                Title = "Rainforest",
                SingerId = singer.SingerId,
            };
            context.Albums.Add(album);
            var track = new Track
            {
                AlbumId = album.AlbumId,
                TrackId = 1,
                Title = "Butterflies",
            };
            context.Tracks.Add(track);
            if (await context.Venues.FindAsync("CON") == null)
            {
                context.Venues.Add(new Venue
                {
                    Code = "CON",
                    Name = "Concert Hall",
                    Active = true,
                });
            }
            var concert = new Concert
            {
                VenueCode = "CON",
                SingerId = singer.SingerId,
                StartTime = new DateTime(2021, 1, 27, 18, 0, 0, DateTimeKind.Utc),
                Title = "Alice Jameson - LIVE in Concert Hall",
            };
            context.Add(concert);

            await context.SaveChangesAsync();
            return (concert, track);
        }
    }
}
