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
using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using Google.Cloud.Spanner.Common.V1;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.ModelValidationTests
{

    /// <summary>
    /// Context that uses a model where the key of a child entity (Album) does not correspond
    /// with an existing primary key or unique index. This should trigger a validation error.
    /// </summary>
    public class InvalidChildEntityDbContext : ValidDbContext
    {
        public InvalidChildEntityDbContext(DatabaseName databaseName) : base(databaseName)
        {
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Singer>(entity =>
            {
                entity.HasKey(e => e.SingerId);
            });

            modelBuilder.Entity<Album>(entity =>
            {
                entity
                    .InterleaveInParent(typeof(Singer), OnDelete.Cascade)
                    .HasKey(entity => new { entity.AlbumId }); // This should have included SingerId.
            });
        }
    }
}
