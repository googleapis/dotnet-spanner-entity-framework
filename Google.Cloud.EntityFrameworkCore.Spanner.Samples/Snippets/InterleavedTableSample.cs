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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

/// <summary>
/// Interleaved tables are treated the same way as FOREIGN KEYS in Entity Framework Core.
/// This means that it is possible to traverse the relationship in both directions, and
/// that deleting a parent record can also automatically delete the child rows.
/// 
/// See https://cloud.google.com/spanner/docs/schema-and-data-model#parent-child_table_relationships
/// for more information on interleaved tables.
/// 
/// Note that normal FOREIGN KEYS in Cloud Spanner do not support ON DELETE CASCADE.
/// 
/// Run from the command line with `dotnet run InterleavedTableSample`
/// </summary>
public static class InterleavedTableSample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);
        var singer = new Singer
        {
            FirstName = "Brian",
            LastName = "Truman",
            Albums = new List<Album>
            {
                new()
                {
                    Title = "Potatoes",
                    // Tracks are interleaved in Albums. This relationship is treated the same as FOREIGN KEYS in
                    // Entity Framework Core, which means that we can traverse the relationship both ways, and any
                    // Track that references an Album that has is associated with the database context will also be
                    // associated with the context.
                    Tracks = new List<Track>
                    {
                        new() {TrackId = 1, Title = "They are good"},
                        new() {TrackId = 2, Title = "Some find them delicious"},
                    }
                }
            }
        };
        await context.Singers.AddAsync(singer);

        // This will save 1 singer, 1 album and 2 tracks.
        var updateCount = await context.SaveChangesAsync();
        Console.WriteLine($"Saved {updateCount} rows");

        // We can traverse the relationship from Track to Album.
        var album = singer.Albums.First();
        foreach (var track in album.Tracks)
        {
            Console.WriteLine($"'{track.Title}' is on album '{track.Album.Title}'");
        }

        // Tracks are defined as `INTERLEAVE IN PARENT Albums ON DELETE CASCADE`. Deleting an
        // album will therefore automatically also delete its tracks.
        context.Albums.Remove(album);
        var deletedCount = await context.SaveChangesAsync();
        Console.WriteLine($"Deleted {deletedCount} albums and tracks");
    }
}
