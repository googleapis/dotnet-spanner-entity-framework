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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel;
using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;
using System.Threading.Tasks;

/// <summary>
/// Sample for using request tags and transaction tags with Spanner and Entity Framework.
/// 
/// Run from the command line with `dotnet run TaggingSample`
/// </summary>
public static class TaggingSample
{
    public static async Task Run(string connectionString)
    {
        await using var context = new SpannerSampleDbContext(connectionString);
        
        // Execute a read/write transaction with a transaction tag.
        await using var transaction = await context.Database.BeginTransactionAsync("my_transaction_tag");
        
        // Insert a singer using the current transaction. This request will include the transaction tag that has been
        // set for the transaction.
        var singerId = Random.Shared.NextInt64();
        await context.Singers.AddAsync(new Singer
        {
            SingerId = singerId,
            FirstName = "Alice",
            LastName = "Goldberg",
        });
        await context.SaveChangesAsync();
        // Count the number of singers with the given ID in the database using the same transaction.
        // This will also automatically include the transaction tag.
        // In addition, we add a request tag to the query.
        var count = await context.Singers
            .WithRequestTag("my_request_tag")
            .Where(s => s.SingerId == singerId)
            .CountAsync();
        Console.WriteLine($"Searching for singer with id {singerId} yielded {count} result(s)");

        await transaction.CommitAsync();
    }
}
