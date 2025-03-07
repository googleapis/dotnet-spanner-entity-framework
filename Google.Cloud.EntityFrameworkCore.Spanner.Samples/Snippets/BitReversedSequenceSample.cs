// Copyright 2024 Google Inc. All Rights Reserved.
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
using System.Text.Json;
using System.Threading.Tasks;

/// <summary>
/// This sample shows how to use a bit-reversed sequence to auto-generate a primary key value
/// for an entity.
/// 
/// Run from the command line with `dotnet run BitReversedSequenceSample`
/// </summary>
public static class BitReversedSequenceSample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);
        var concert = await CreateDependentEntitiesAsync(context);

        // Create a new TicketSale, add it to the context, and save the changes.
        // TicketSale uses a bit-reversed sequence for primary key generation.
        // The Id column has a DEFAULT constraint that automatically
        // fetches the next value from the bit-reversed sequence.
        var ticketSale = await context.TicketSales.AddAsync(new TicketSale
        {
            CustomerName = "Lamar Chavez",
            Seats = ["A10", "A11", "A12"],
            Concert = concert,
        });
        var count = await context.SaveChangesAsync();
        
        // SaveChangesAsync returns the total number of rows that was inserted/updated/deleted.
        // It also automatically populates the generated primary key property.
        Console.WriteLine($"Added {count} ticket sale with id {ticketSale.Entity.Id}.");

        // Inserting multiple records in one batch (and in one transaction) is also supported.
        TicketSale[] ticketSales =
        [
            new TicketSale
            {
                CustomerName = "Reid Phillips",
                Seats = ["B1", "B2"],
                Concert = concert,
            },
            new TicketSale
            {
                CustomerName = "Jaime Diaz",
                Seats = ["C76"],
                Concert = concert,
            },
            new TicketSale
            {
                CustomerName = "Lindsay Yates",
                Seats = ["A13", "A14", "A15", "A16"],
                Concert = concert,
            },
        ];
        var transaction = await context.Database.BeginTransactionAsync();
        await context.AddRangeAsync(ticketSales);
        var batchCount = await context.SaveChangesAsync();
        await transaction.CommitAsync();
        
        // SaveChangesAsync returns the total number of rows that was inserted/updated/deleted.
        // It also automatically populates the generated primary key property.
        Console.WriteLine($"Added {batchCount} ticket sale with identifiers {string.Join(", ", ticketSales.Select(ts => ts.Id))}.");
    }
    

    private static async Task<Concert> CreateDependentEntitiesAsync(SpannerSampleDbContext context)
    {
        var singer = new Singer
        {
            FirstName = "Alice",
            LastName = "Jameson",
            Albums = new List<Album>
            {
                new() { Title = "Rainforest", Tracks = new List<Track>
                {
                    new() {TrackId = 1, Title = "Butterflies"},
                }}
            }
        };
        await context.Singers.AddAsync(singer);
        if (await context.Venues.FindAsync("CON") == null)
        {
            await context.Venues.AddAsync(new Venue
            {
                Code = "CON",
                Name = "Concert Hall",
                Description = JsonDocument.Parse("{\"Capacity\": 1000, \"Type\": \"Building\"}"),
                Active = true,
            });
        }
        var concert = new Concert
        {
            VenueCode = "CON",
            Singer = singer,
            StartTime = new DateTime(2021, 1, 27, 18, 0, 0, DateTimeKind.Utc),
            Title = "Alice Jameson - LIVE in Concert Hall",
        };
        await context.AddAsync(concert);

        await context.SaveChangesAsync();
        return concert;
    }    
}
