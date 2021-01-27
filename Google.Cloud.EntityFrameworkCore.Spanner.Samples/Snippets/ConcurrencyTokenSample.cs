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

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.Snippets
{
    /// <summary>
    /// Entity Framework Core supports concurrency handling using concurrency tokens:
    /// https://docs.microsoft.com/en-us/ef/core/saving/concurrency
    /// 
    /// The sample model contains an abstract base entity VersionedEntity with one property 'Version'.
    /// All concrete entities extend from this base entity and use the 'Version' property as a concurrency
    /// token. This concurrency token is automatically increased by the model every time an entity is updated.
    /// This means that if a row was updated by a different process between the moment that this process read
    /// it and tries to write it, the other process will have updated the version number of the row and the
    /// update by this process will fail with a <see cref="DbUpdateConcurrencyException"/>.
    /// </summary>
    public static class ConcurrencyTokenSample
    {
        public static async Task Run(string connectionString)
        {
            await CreateSampleRow(connectionString);

            using var context = new SpannerSampleDbContext(connectionString);

            // Read a venue from the database.
            var code = "CON";
            var venue = await context.Venues.FindAsync(code);

            // Simulate a concurrent update by a different process by executing a manual update.
            await context.Database.ExecuteSqlInterpolatedAsync($"UPDATE Venues SET Version={venue.Version + 1} WHERE Code={code}");

            // Now try to update the venue entity. This will fail as the version number has been changed
            // between the time the entity was read from the database and the time it is written.
            venue.Name = $"{venue.Name} - Renovated";
            try
            {
                // This will fail.
                await context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException e)
            {
                // In this case we know that there is only one entry and that it is a Venue.
                var venueEntry = e.Entries[0];

                // Just update the original values with the values in the database and retry
                // the save changes call. This should now succeed. A real application should
                // consider which value should be used, or even propagate the error to let the
                // end user decide which value should be used.
                // See https://docs.microsoft.com/en-us/ef/core/saving/concurrency for more
                // information on how to handle concurrency conflicts.
                venueEntry.OriginalValues.SetValues(await venueEntry.GetDatabaseValuesAsync());
                await context.SaveChangesAsync();
                Console.WriteLine($"Force update of the name of venue with code {venue.Code} to {venue.Name}");
            }
        }

        private async static Task CreateSampleRow(string connectionString)
        {
            using var context = new SpannerSampleDbContext(connectionString);
            if (await context.Venues.FindAsync("CON") != null)
            {
                return;
            }
            context.Venues.Add(new Venue
            {
                Code = "CON",
                Name = "Concert Hall",
                Active = true,
            });
            await context.SaveChangesAsync();
        }
    }
}
