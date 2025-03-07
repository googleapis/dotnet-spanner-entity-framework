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
/// Cloud Spanner supports storing ARRAYs of each of its base types.
/// ARRAY types are by default mapped to <see cref="List{T}"/> in Entity Framework.
/// 
/// Run from the command line with `dotnet run ArraysSample`
/// </summary>
public static class ArraysSample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);
        var (singer, album) = await GetSingerAndAlbumAsync(context);

        // A track has two array columns: Lyrics and LyricsLanguages. The length of both arrays
        // should be equal, as the LyricsLanguages indicate the language of the corresponding Lyrics.
        var track1 = new Track
        {
            AlbumId = album.AlbumId,
            TrackId = 1,
            Title = "Whenever",
            Lyrics = new List<string> { "Lyrics 1", "Lyrics 2" },
            LyricsLanguages = new List<string> { "EN", "DE" },
        };
        var track2 = new Track
        {
            AlbumId = album.AlbumId,
            TrackId = 2,
            Title = "Wherever",
            // Array elements may be null, regardless whether the column itself is defined as NULL/NOT NULL.
            Lyrics = new List<string> { null, "Lyrics 2" },
            LyricsLanguages = new List<string> { "EN", "DE" },
        };
        var track3 = new Track
        {
            AlbumId = album.AlbumId,
            TrackId = 3,
            Title = "Probably",
            // ARRAY columns may also be null.
            Lyrics = null,
            LyricsLanguages = null,
        };
        await context.Tracks.AddRangeAsync(track1, track2, track3);
        var count = await context.SaveChangesAsync();

        Console.WriteLine($"Added {count} tracks.");

        // TODO: Add sample for querying using array functions.
    }

    private static async Task<(Singer, Album)> GetSingerAndAlbumAsync(SpannerSampleDbContext context)
    {
        var singer = new Singer
        {
            FirstName = "Hannah",
            LastName = "Polansky",
            Albums = new List<Album>
            {
                new() { Title = "Somewhere" }
            }
        };
        await context.Singers.AddAsync(singer);
        await context.SaveChangesAsync();

        return (singer, singer.Albums.First());
    }
}
