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
/// Sample for executing a read-only transaction on Spanner through Entity Framework.
/// Prefer read-only transactions over read/write transactions if you need to execute
/// multiple consistent reads and no write operations.
/// 
/// Run from the command line with `dotnet run ReadOnlyTransactionSample`
/// </summary>
public static class ReadOnlyTransactionSample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);

        // Start a read-only transaction with strong timestamp bound (i.e. read everything that has been committed up until now).
        // This transaction will be assigned a read-timestamp at the first read that it executes and all
        // following read operations will also use the same read timestamp. Any changes that are made after
        // this read timestamp will not be visible to the transaction.
        // NOTE: Although read-only transaction cannot be committed or rolled back, they still need to be disposed.
        using var transaction = await context.Database.BeginReadOnlyTransactionAsync(TimestampBound.Strong);

        // Search for a singer with a new id. This will establish a read timestamp for the read-only transaction.
        var singerId = Random.Shared.NextInt64();
        var count = await context.Singers
            .Where(s => s.SingerId == singerId)
            .CountAsync();
        Console.WriteLine($"Searching for singer with id {singerId} yielded {count} result(s)");

        // Create a new database context and insert a singer with the given id. This singer will not be visible
        // to the read-only transaction.
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

        // The count should not have changed, as the read-only transaction will continue to use
        // the read timestamp assigned during the first read.
        count = await context.Singers
            .Where(s => s.SingerId == singerId)
            .CountAsync();
        Console.WriteLine($"Searching for singer with id {singerId} yielded {count} result(s)");

        // Now 'commit' the read-only transaction. This will close the transaction and allow us to start
        // a new one on the context.
        await transaction.CommitAsync();

        // Start a new read-only transaction. TimestampBound.Strong is default so we don't have to specify it.
        using var newTransaction = await context.Database.BeginReadOnlyTransactionAsync();
        count = await context.Singers
            .Where(s => s.SingerId == singerId)
            .CountAsync();
        Console.WriteLine($"Searching for singer with id {singerId} yielded {count} result(s)");
    }
}
