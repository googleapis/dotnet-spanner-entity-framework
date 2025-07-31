// Copyright 2025 Google Inc. All Rights Reserved.
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
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

/// <summary>
/// Simple sample showing how to map a POCO to a JSON column.
/// 
/// Run from the command line with `dotnet run StructuralJsonSample`
/// </summary>
public static class StructuralJsonSample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);

        // Create a new Venue with two VenueDescriptions.
        // VenueDescriptions are mapped to a single JSON column.
        await context.Venues.AddAsync(new Venue
        {
            Code = "CH",
            Name = "Concert Hall",
            Descriptions =
            [
                new() { Active = true, Category = "Concert Hall", Capacity = 1000, Description = "Large Concert Hall" },
                new() { Active = false, Category = "Hall", Capacity = 1000, Description = "Large Hall" }
            ]
        });
        var count = await context.SaveChangesAsync();

        // SaveChangesAsync returns the total number of rows that was inserted/updated/deleted.
        Console.WriteLine($"Added {count} venue.");
        
        // Read back the Venue that we added and iterate over the VenueDescriptions that were serialized as JSON in the database.
        var venue = await context.Venues.FirstAsync();
        Console.WriteLine($"Venue has {venue.Descriptions.Count} descriptions.");
        foreach (var description in venue.Descriptions)
        {
            Console.WriteLine($"{JsonSerializer.Serialize(description)}");
        }
    }
}
