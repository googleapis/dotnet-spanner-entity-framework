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
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Samples.Snippets
{
    /// <summary>
    /// The Clr type <see cref="DateTime"/> is often used for both dates and timestamps. Cloud Spanner has two distinct
    /// data types for DATE and TIMESTAMP. To distinguish between the two in Entity Framework Core, it is recommended to
    /// use <see cref="SpannerDate"/> to map DATE columns and <see cref="DateTime"/> to map TIMESTAMP columns.
    /// </summary>
    public static class DateSample
    {
        public static async Task Run(string connectionString)
        {
            using var context = new SpannerSampleDbContext(connectionString);
            var singer = new Singer
            {
                SingerId = Guid.NewGuid(),
                FirstName = "Yvette",
                LastName = "Wendelson",
                // SpannerDate is specifically designed to map a DATE column in a Cloud Spanner
                // database to a property of an entity. DateTime properties will by default be
                // mapped to TIMESTAMP columns.
                BirthDate = new SpannerDate(1980, 10, 17),
            };
            context.Singers.Add(singer);
            await context.SaveChangesAsync();

            // Commonly used properties and methods of SpannerDate are mapped to the equivalent Cloud Spanner functions.
            var singersBornIn1980 = await context.Singers
                .Where(s => s.BirthDate.GetValueOrDefault().Year == 1980)
                .OrderBy(s => singer.LastName)
                .ToListAsync();
            foreach (var s in singersBornIn1980)
            {
                Console.WriteLine($"Born in 1980: {s.FullName} ({s.BirthDate})");
            }
        }
    }
}
