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
using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;
using System.Threading.Tasks;

/// <summary>
/// The Clr type <see cref="DateTime"/> is often used for both dates and timestamps. Cloud Spanner has two distinct
/// data types for DATE and TIMESTAMP. To distinguish between the two in Entity Framework Core, it is recommended to
/// use <see cref="SpannerDate"/> to map DATE columns and <see cref="DateTime"/> to map TIMESTAMP columns.
/// 
/// Run from the command line with `dotnet run TimestampSample`
/// </summary>
public static class TimestampSample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);

        // Create a concert.
        var (singer, venue) = await GetSingerAndVenueAsync(connectionString);
        var concert = new Concert
        {
            SingerId = singer.SingerId,
            VenueCode = venue.Code,
            // TIMESTAMP columns are mapped to DateTime by default. Cloud Spanner stores all TIMESTAMP values in UTC.
            // If a TIMESTAMP value is set in local time, the value will be converted to UTC before it is written to
            // Cloud Spanner.
            StartTime = new DateTime(2021, 2, 1, 19, 30, 0, DateTimeKind.Utc),
            Title = "Theodore in Concert Hall",
        };
        await context.Concerts.AddAsync(concert);
        await context.SaveChangesAsync();

        // Commonly used methods and properties of DateTime are mapped to the equivalent Cloud Spanner functions and can be used in queries.
        var concertsInFeb2021 = await context.Concerts
            .Where(c => c.StartTime.Month == 2 && c.StartTime.Year == 2021)
            .OrderBy(c => c.StartTime)
            .ToListAsync();
        foreach (var c in concertsInFeb2021)
        {
            Console.WriteLine($"February concert: {c.Title}, starts at {c.StartTime}");
        }
    }

    private static async Task<(Singer, Venue)> GetSingerAndVenueAsync(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);
        using var transaction = await context.Database.BeginTransactionAsync();
        var venue = await context.Venues.FindAsync("CON");
        if (venue == null)
        {
            venue = new Venue
            {
                Code = "CON",
                Name = "Concert Hall",
                Active = true,
            };
            await context.Venues.AddAsync(venue);
        }
        var singer = new Singer
        {
            SingerId = Guid.NewGuid(),
            FirstName = "Theodore",
            LastName = "Walterson",
        };
        await context.Singers.AddAsync(singer);
        await context.SaveChangesAsync();
        await transaction.CommitAsync();

        return (singer, venue);
    }
}
