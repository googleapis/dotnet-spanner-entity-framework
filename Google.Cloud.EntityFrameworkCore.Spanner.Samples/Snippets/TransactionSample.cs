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
using System.Threading.Tasks;

/// <summary>
/// By default all changes in a single call to SaveChanges are applied in a transaction.
/// 
/// You can also manually control transactions if you want to group multiple SaveChanges
/// in a single transaction.
/// 
/// See https://docs.microsoft.com/en-us/ef/core/saving/transactions for more information
/// on how to control transactions with Entity Framework Core.
/// 
/// NOTE: Cloud Spanner does not support Savepoints.
/// 
/// Run from the command line with `dotnet run TransactionSample`
/// </summary>
public static class TransactionSample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);

        // Start a read/write transaction that will be used with the database context.
        using var transaction = await context.Database.BeginTransactionAsync();

        // Create a new Singer, add it to the context and save the changes.
        // These changes have not yet been committed to the database and are
        // therefore not readable for other processes.
        var entry = await context.Singers.AddAsync(new Singer
        {
            FirstName = "Bernhard",
            LastName = "Bennet"
        });
        var count = await context.SaveChangesAsync();
        Console.WriteLine($"Added {count} singer in a transaction.");
        var singerId = entry.Entity.SingerId;

        // Now try to read the singer in a different context which will use a different transaction.
        // This will return null, as pending changes from other transactions are not visible.
        using var contextWithoutTransaction = new SpannerSampleDbContext(connectionString);
        var exists = await contextWithoutTransaction.Singers
            .FromSqlInterpolated($"SELECT * FROM Singers WHERE SingerId={singerId}")
            .FirstOrDefaultAsync();
        Console.WriteLine($"Can read singer outside of transaction: {exists != null}");

        // Now try to read the same using the context with the transaction. This will return true as
        // a transaction can read its own writes. The Cloud Spanner Entity Framework Core provider
        // uses DML by default for updates that are executed in manual transactions in order to support
        // the read-your-writes feature.
        exists = await context.Singers
            .FromSqlInterpolated($"SELECT * FROM Singers WHERE SingerId={singerId}")
            .FirstOrDefaultAsync();
        Console.WriteLine($"Can read singer inside transaction: {exists != null}");

        // Commit the transaction. The singer is now also readable in a context without the transaction.
        await transaction.CommitAsync();

        exists = await contextWithoutTransaction.Singers
            .FromSqlInterpolated($"SELECT * FROM Singers WHERE SingerId={singerId}")
            .FirstOrDefaultAsync();
        Console.WriteLine($"Can read singer after commit: {exists != null}");
    }
}
