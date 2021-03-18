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
/// Linq can be used to query entities from Cloud Spanner.
/// Commonly used properties and methods are mapped to the corresponding functions in Cloud Spanner.
/// 
/// Run from the command line with `dotnet run QueryWithLinqSample`
/// </summary>
public static class QueryWithLinqSample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);
        await Setup(context);

        var singersBornBefore2000 = context.Singers
            .Where(s => s.BirthDate.GetValueOrDefault().Year < 2000)
            .OrderBy(s => s.BirthDate)
            .AsAsyncEnumerable();
        Console.WriteLine("Singers born before 2000:");
        await foreach (var singer in singersBornBefore2000)
        {
            Console.WriteLine($"{singer.FullName}, born at {singer.BirthDate}");
        }

        var singersStartingWithAl = context.Singers
            .Where(s => s.FullName.StartsWith("Al"))
            .OrderBy(s => s.LastName)
            .AsAsyncEnumerable();
        Console.WriteLine("Singers with a name starting with 'Al':");
        await foreach (var singer in singersStartingWithAl)
        {
            Console.WriteLine($"{singer.FullName}");
        }
    }

    private static async Task Setup(SpannerSampleDbContext context)
    {
        await context.Singers.AddRangeAsync(
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
