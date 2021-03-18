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
using System;
using System.Threading.Tasks;

/// <summary>
/// Cloud Spanner supports generated columns: https://cloud.google.com/spanner/docs/generated-column/how-to
/// These properties must be annotated with 'ValueGeneratedOnAddOrUpdate' in the Entity Framework Model.
/// 
/// Run from the command line with `dotnet run GeneratedColumnSample`
/// </summary>
public static class GeneratedColumnSample
{
    public static async Task Run(string connectionString)
    {
        using var context = new SpannerSampleDbContext(connectionString);

        // Singer has a generated column FullName that is the combination of the
        // FirstName and LastName. The value is automatically computed by Cloud Spanner.
        // Setting it manually client side has no effect.
        var singer = new Singer
        {
            SingerId = Guid.NewGuid(),
            FirstName = "Alice",
            LastName = "Jameson"
        };
        await context.Singers.AddAsync(singer);
        await context.SaveChangesAsync();

        // Entity Framework will automatically fetch the computed value for FullName
        // from Cloud Spanner after it has been written.
        Console.WriteLine($"Added singer with full name {singer.FullName}");

        // Updating the last name of the singer will also update the full name.
        singer.LastName = "Jameson - Cooper";
        await context.SaveChangesAsync();
        Console.WriteLine($"Updated singer's full name to {singer.FullName}");
    }
}
