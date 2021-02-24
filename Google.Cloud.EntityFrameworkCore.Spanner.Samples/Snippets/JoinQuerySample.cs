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
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;
using System.Threading.Tasks;

/// <summary>
/// Linq supports the JOIN operator which translates to the INNER JOIN relational operator.
/// 
/// Run from the command line with `dotnet run JoinQuerySample`
/// </summary>
public static class JoinQuerySample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);
        await Setup(context);

        var query = from album in context.Albums
                    join singer in context.Singers
                        on album.SingerId equals singer.SingerId
                    select new { singer, album };

        Console.WriteLine("Singers and albums:");
        await foreach(var row in query.AsAsyncEnumerable())
        {
            Console.WriteLine($"Singer {row.singer.FullName} produced album {row.album.Title}");
        }
    }

    private static async Task Setup(SpannerSampleDbContext context)
    {
        var singer = new Singer
        {
            SingerId = Guid.NewGuid(),
            FirstName = "Alice",
            LastName = "Henderson",
            BirthDate = new SpannerDate(1983, 10, 19),
        };
        context.Singers.Add(singer);
        context.Albums.AddRange(new Album
        {
            AlbumId = Guid.NewGuid(),
            SingerId = singer.SingerId,
            Title = "Henderson's first",
        },
        new Album
        {
            AlbumId = Guid.NewGuid(),
            SingerId = singer.SingerId,
            Title = "Henderson's second",
        },
        new Album
        {
            AlbumId = Guid.NewGuid(),
            SingerId = singer.SingerId,
            Title = "Henderson's third",
        });
        await context.SaveChangesAsync();
    }
}
