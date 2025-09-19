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
using Microsoft.EntityFrameworkCore;
using System;
using System.Data;
using System.Threading.Tasks;

/// <summary>
/// By default, Spanner uses external consistency as the isolation level for read/write transactions.
/// This level is slightly stricter than the Serializable isolation level that is defined by the SQL standard.
/// See https://cloud.google.com/spanner/docs/true-time-external-consistency for more information.
/// 
/// Using a less strict isolation level can improve performance, reduce lock conflicts, and reduce the number
/// of transactions that are aborted by Spanner.
///
/// This sample shows how to set the default isolation level for a connection, and how to specify a custom
/// isolation level for a single transaction.
/// 
/// Run from the command line with `dotnet run IsolationLevelSample`
/// </summary>
public static class IsolationLevelSample
{
    public static async Task Run(string connectionString)
    {
        // Set the default isolation level to RepeatableRead for all transactions that are executed by this
        // connection. This default can be overridden by specifying a custom isolation level when calling the
        // BeginTransaction method.
        // If no default isolation level is set in the connection string, then Spanner will use Serializable
        // as the default isolation level.
        await using var context = new SpannerSampleDbContext(connectionString + ";IsolationLevel=RepeatableRead");

        // Start a read/write transaction that will be used with the database context.
        // This transaction will use isolation level RepeatableRead.
        await using var transactionRepeatableRead = await context.Database.BeginTransactionAsync();
        await context.Singers.AddAsync(new Singer
        {
            FirstName = "Bernhard",
            LastName = "Bennet"
        });
        var count = await context.SaveChangesAsync();
        await transactionRepeatableRead.CommitAsync();
        Console.WriteLine($"Added {count} singer in a transaction using RepeatableRead isolation level.");
        
        // Start a read/write transaction with a specific isolation level.
        await using var transactionSerializable = await context.Database.BeginTransactionAsync(IsolationLevel.Serializable);
        await context.Singers.AddAsync(new Singer
        {
            FirstName = "Alice",
            LastName = "Robinson"
        });
        count = await context.SaveChangesAsync();
        await transactionSerializable.CommitAsync();
        Console.WriteLine($"Added {count} singer in a transaction using Serializable isolation level.");
    }
}
