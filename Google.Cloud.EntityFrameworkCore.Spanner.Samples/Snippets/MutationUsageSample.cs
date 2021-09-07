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
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.Samples.SampleModel;
using Microsoft.EntityFrameworkCore;
using System;
using System.Threading.Tasks;

/// <summary>
/// By default the Cloud Spanner Entity Framework Core provider will use mutations for
/// updates that are executed in an implicit transaction, and DML for updates that are
/// executed in a manual transaction. The reason for this default behavior is that:
/// 1. Mutations are more efficient for multiple small insert/update/delete statements.
///    Mutations do not support read-your-writes semantics. The lack of this feature is
///    not a problem for implicit transactions, as it is impossible to execute queries
///    in an implicit transaction.
/// 2. DML statements are less efficient than mutations for multiple small updates, but
///    they do support read-your-writes semantics. Manual transactions can span multiple
///    statements and queries, and the lack of read-your-writes would negatively impact
///    the usefulness of manual transactions.
/// 
/// An application can configure a DbContext to use mutations or DML statements for all
/// updates, instead of using one type for implicit transactions and the other for manual
/// transactions. Changing the default behavior will have the following impact on your
/// application:
/// 1. Configuring a DbContext to always use Mutations: This will speed up the execution
///    speed of large batches of inserts/updates/deletes, but it will also mean that the
///    application will not be able to read its own writes during a manual transaction.
/// 2. Configuring a DbContext to always use DML: This will reduce the execution speed of
///    large batches of inserts/updates/deletes that are executed as implicit transactions.
/// 
/// Run from the command line with `dotnet run MutationUsageSample`
/// </summary>
public static class MutationUsageSample
{
    /// <summary>
    /// A sample DbContext that supports manual configuration of when to use mutations.
    /// </summary>
    internal class SpannerSampleMutationUsageDbContext : SpannerSampleDbContext
    {
        internal MutationUsage MutationUsage { get; }

        internal SpannerSampleMutationUsageDbContext(string connectionString, MutationUsage mutationUsage) : base(connectionString)
        {
            MutationUsage = mutationUsage;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder options)
        {
            base.OnConfiguring(options);
            options.UseMutations(MutationUsage);
        }
    }

    public static async Task Run(string connectionString)
    {
        // Create a DbContext that uses Mutations for implicit transactions. This is the default behavior of a DbContext.
        // This means that the context will use mutations for implicit transactions, and DML for manual transactions.
        await RunSampleStatements(new SpannerSampleMutationUsageDbContext(connectionString, MutationUsage.ImplicitTransactions));

        // Create a DbContext that always uses Mutations. This means that read-your-writes will be disabled for manual
        // transactions. It also means that large batches of inserts/updates/deletes will execute faster. It is recommended
        // to use this mode for manual transactions that do not need read-your-writes, and that do contain larege update batches.
        await RunSampleStatements(new SpannerSampleMutationUsageDbContext(connectionString, MutationUsage.Always));

        // Create a DbContext that never uses Mutations. All inserts/updates/deletes will be executed as DML statements.
        await RunSampleStatements(new SpannerSampleMutationUsageDbContext(connectionString, MutationUsage.Never));
    }

    private static async Task RunSampleStatements(SpannerSampleMutationUsageDbContext context)
    {
        Console.WriteLine();
        Console.WriteLine($"Running sample with mutation usage {context.MutationUsage}");
        // Add a new singer using an implicit transaction.
        var singerId = Guid.NewGuid();
        await context.Singers.AddAsync(new Singer
        {
            SingerId = singerId,
            FirstName = "Bernhard",
            LastName = "Bennet"
        });
        var count = await context.SaveChangesAsync();
        Console.WriteLine($"Added {count} singer in an implicit transaction with a context that has mutation usage {context.MutationUsage}.");

        // Now try to read the row back using the same context. This will always return true.
        var exists = await context.Singers
            .FromSqlInterpolated($"SELECT * FROM Singers WHERE SingerId={singerId}")
            .FirstOrDefaultAsync();
        Console.WriteLine($"Can read singer after implicit transaction: {exists != null}");

        // Start a read/write transaction that will be used with the database context.
        using var transaction = await context.Database.BeginTransactionAsync();

        // Create a new Singer, add it to the context and save the changes.
        // These changes have not yet been committed to the database and are
        // therefore not readable for other processes. It will be readable for
        // the same transaction, unless MutationUsage has been set to Always.
        singerId = Guid.NewGuid();
        await context.Singers.AddAsync(new Singer
        {
            SingerId = singerId,
            FirstName = "Alice",
            LastName = "Wendelson"
        });
        count = await context.SaveChangesAsync();
        Console.WriteLine($"Added {count} singer in a manual transaction with a context that has mutation usage {context.MutationUsage}.");

        // Now try to read the row back using the same context. This will return true for contexts that use
        // DML for manual transactions.
        exists = await context.Singers
            .FromSqlInterpolated($"SELECT * FROM Singers WHERE SingerId={singerId}")
            .FirstOrDefaultAsync();
        Console.WriteLine($"Can read singer inside transaction: {exists != null}");

        // Commit the transaction. The singer is now always readable.
        await transaction.CommitAsync();

        exists = await context.Singers
            .FromSqlInterpolated($"SELECT * FROM Singers WHERE SingerId={singerId}")
            .FirstOrDefaultAsync();
        Console.WriteLine($"Can read singer after commit: {exists != null}");
    }
}
