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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;
using System.Threading.Tasks;

/// <summary>
/// Sample for executing a stale read on Spanner through Entity Framework without
/// an explicit read-only transaction.
/// 
/// Run from the command line with `dotnet run StaleReadSample`
/// </summary>
public static class StaleReadSample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);
        
        // Get the current timestamp on the backend.
        using var cmd = context.Database.GetDbConnection().CreateCommand();
        cmd.CommandText = "SELECT CURRENT_TIMESTAMP";
        var timestamp = (DateTime) await cmd.ExecuteScalarAsync();

        // Search for a singer with a new id. This singer will not be found.
        var singerId = Guid.NewGuid();
        var count = await context.Singers
            .Where(s => s.SingerId == singerId)
            .CountAsync();
        Console.WriteLine($"Searching for singer with id {singerId} yielded {count} result(s)");

        // Create a new database context and insert a singer with the given id.
        using (var writeContext = new SpannerSampleDbContext(connectionString))
        {
            await writeContext.Singers.AddAsync(new Singer
            {
                SingerId = singerId,
                FirstName = "Alice",
                LastName = "Goldberg",
            });
            await writeContext.SaveChangesAsync();
        }
        
        // Now execute a stale read on the Singers table at a timestamp that is before the singer was inserted.
        // The count should be 0.
        count = await context.Singers
            .WithTimestampBound(TimestampBound.OfReadTimestamp(timestamp))
            .Where(s => s.SingerId == singerId)
            .CountAsync();
        Console.WriteLine($"Searching with read timestamp #{timestamp:yyyy-MM-ddTHH:mm:ss.fffffffZ} for singer with id {singerId} yielded {count} result(s)");

        // Try to read the row with a strong timestamp bound.
        count = await context.Singers
            .WithTimestampBound(TimestampBound.Strong)
            .Where(s => s.SingerId == singerId)
            .CountAsync();
        Console.WriteLine($"Searching with a strong timestamp bound for singer with id {singerId} yielded {count} result(s)");
        
        // Try to read the singer with a max staleness. The result of this is non-deterministic, as the backend
        // may choose the read timestamp, as long as it is no older than the specified max staleness.
        count = await context.Singers
            .WithTimestampBound(TimestampBound.OfMaxStaleness(TimeSpan.FromSeconds(1)))
            .Where(s => s.SingerId == singerId)
            .CountAsync();
        Console.WriteLine($"Searching with a max staleness for singer with id {singerId} yielded {count} result(s)");
    }
}
