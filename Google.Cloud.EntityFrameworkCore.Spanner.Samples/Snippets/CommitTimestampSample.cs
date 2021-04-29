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

/// <summary>
/// Cloud Spanner supports writing the commit timestamp of a row: https://cloud.google.com/spanner/docs/commit-timestamp
/// 
/// See the <see cref="Performance"/> entity in <see cref="SpannerSampleDbContext.OnModelCreating(Microsoft.EntityFrameworkCore.ModelBuilder)"/>
/// for the annotations that need to be set on the entity to automatically fill a commit timestamp value.
/// 
/// This feature has one limitation that should be taken into consideration before using it:
/// When writing a PENDING_COMMIT_TIMESTAMP to a table, that column will be unreadable for ALL rows in the table
/// for the remainder of the transaction. This means that you cannot execute any queries that reference this
/// column after calling SaveChanges during the same transaction.
/// NOTE: This is ONLY an issue if you use MANUAL transactions.
/// 
/// See also https://cloud.google.com/spanner/docs/commit-timestamp#dml
/// 
/// Run from the command line with `dotnet run CommitTimestampSample`
/// </summary>
public static class CommitTimestampSample
{
    public static async Task Run(string connectionString)
    {
        var startTime = new DateTime(2021, 1, 27, 19, 0, 0, DateTimeKind.Utc);
        using var context = new SpannerSampleDbContext(connectionString);
        (Concert concert, Track track) = await GetConcertAndTrackAsync(context);

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
        await context.Performances.AddAsync(performance);
        var count = await context.SaveChangesAsync();
        Console.WriteLine($"Saved {count} performance");
        Console.WriteLine($"Performance was created at {performance.CreatedAt}");

        // Last updated is only filled when the entity is updated (not when it is inserted).
        var lastUpdated = performance.LastUpdatedAt == null ? "<never>" : performance.LastUpdatedAt.ToString();
        Console.WriteLine($"Performance was last updated at {lastUpdated}");

        // Update the performance. This will also fill the LastUpdatedAt property.
        performance.Rating = 8.5;
        count = await context.SaveChangesAsync();
        Console.WriteLine($"Updated {count} performance");
        Console.WriteLine($"Performance was created at {performance.CreatedAt}");
        Console.WriteLine($"Performance was updated at {performance.LastUpdatedAt}");
    }

    private static async Task<(Concert, Track)> GetConcertAndTrackAsync(SpannerSampleDbContext context)
    {
        var singer = new Singer
        {
            SingerId = Guid.NewGuid(),
            FirstName = "Alice",
            LastName = "Jameson",
        };
        await context.Singers.AddAsync(singer);
        var album = new Album
        {
            AlbumId = Guid.NewGuid(),
            Title = "Rainforest",
            SingerId = singer.SingerId,
        };
        await context.Albums.AddAsync(album);
        var track = new Track
        {
            AlbumId = album.AlbumId,
            TrackId = 1,
            Title = "Butterflies",
        };
        await context.Tracks.AddAsync(track);
        if (await context.Venues.FindAsync("CON") == null)
        {
            await context.Venues.AddAsync(new Venue
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
        await context.AddAsync(concert);

        await context.SaveChangesAsync();
        return (concert, track);
    }
}
