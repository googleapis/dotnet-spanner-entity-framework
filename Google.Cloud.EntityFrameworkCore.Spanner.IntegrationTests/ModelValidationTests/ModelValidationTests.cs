// Copyright 2021, Google Inc. All rights reserved.
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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using System;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable EF1001
namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.ModelValidationTests
{
    /// <summary>
    /// Integration tests for model validation against an actual database. These tests cannot execute
    /// in parallel with the other integration tests, as model validation against a database is only
    /// supported as long as the provider is only being used for one database. As each test uses its
    /// own database, this is not the case for integration tests.
    /// </summary>
    [Collection(nameof(NonParallelTestCollection))]
    public class ModelValidationTests : IClassFixture<ModelValidationTestFixture>
    {
        private readonly ModelValidationTestFixture _fixture;

        public ModelValidationTests(ModelValidationTestFixture fixture)
        {
            // Reset the connection string provider for model validation. This ensures that the validation
            // is executed regardless whether these tests execute before or after the other integration tests.
            SpannerModelValidationConnectionProvider.Instance.Reset();
            SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(true);
            _fixture = fixture;
        }

        [Fact]
        public async Task ValidationSucceedsForDefaultModel()
        {
            using var db = new ValidDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.Add(new Singer
            {
                SingerId = singerId,
                FirstName = "Alice",
                LastName = "Tennet",
            });
            Assert.Equal(1, await db.SaveChangesAsync());
        }

        [Fact]
        public void ValidationFailsForModelWithInvalidEntity()
        {
            using var db = new InvalidEntityDbContext(_fixture.DatabaseName);
            Assert.Throws<InvalidOperationException>(() => db.Singers.Add(new Singer { }));
        }

        [Fact]
        public void ValidationFailsForModelWithInvalidChildEntity()
        {
            using var db = new InvalidChildEntityDbContext(_fixture.DatabaseName);
            Assert.Throws<InvalidOperationException>(() => db.Singers.Add(new Singer { }));
        }

        [Fact]
        public async Task ValidationSucceedsForModelWithUniqueIndexAsKey()
        {
            using var db = new ModelWithUniqueIndexAsKeyDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            var albumId1 = _fixture.RandomLong();
            var albumId2 = _fixture.RandomLong();
            db.Singers.Add(new Singer
            {
                SingerId = singerId,
                FirstName = "Nick",
                LastName = "Bennetson",
            });
            db.Albums.AddRange(
                new Album
                {
                    SingerId = singerId,
                    AlbumId = albumId1,
                    Title = $"Random title {albumId1}",
                },
                new Album
                {
                    SingerId = singerId,
                    AlbumId = albumId2,
                    Title = $"Random title {albumId2}",
                });
            Assert.Equal(3, await db.SaveChangesAsync());
            // Verify that we can fetch an album using a (SingerId, Title) key.
            // We do that in a new context to be sure that we actually fetch it from
            // the database and not only the in-memory database context.
            using var context = new ModelWithUniqueIndexAsKeyDbContext(_fixture.DatabaseName);
            var album = await context.Albums.FindAsync(singerId, $"Random title {albumId2}");
            Assert.Equal(albumId2, album.AlbumId);
        }
    }
}
#pragma warning restore EF1001
