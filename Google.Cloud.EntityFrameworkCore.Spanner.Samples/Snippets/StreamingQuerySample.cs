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
/// Shows how to use ToAsyncEnumerable with query results from Cloud Spanner.
/// 
/// Run from the command line with `dotnet run StreamingQuerySample`
/// </summary>
public static class StreamingQuerySample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);
        await Setup(context);

        var singers = context.Singers
            .OrderBy(s => s.BirthDate)
            .AsAsyncEnumerable();
        Console.WriteLine("Found singers:");
        await foreach (var singer in singers)
        {
            Console.WriteLine($"{singer.FullName}, born at {singer.BirthDate}");
        }
    }

    private static async Task Setup(SpannerSampleDbContext context)
    {
        context.Singers.AddRange(
        new Singer
        {
            SingerId = Guid.NewGuid(),
            FirstName = "Alice",
            LastName = "Henderson",
            BirthDate = new SpannerDate(1983, 10, 19),
        },
        new Singer
        {
            SingerId = Guid.NewGuid(),
            FirstName = "Peter",
            LastName = "Allison",
            BirthDate = new SpannerDate(2000, 5, 2),
        },
        new Singer
        {
            SingerId = Guid.NewGuid(),
            FirstName = "Mike",
            LastName = "Nicholson",
            BirthDate = new SpannerDate(1976, 8, 31),
        });
        await context.SaveChangesAsync();
    }
}
