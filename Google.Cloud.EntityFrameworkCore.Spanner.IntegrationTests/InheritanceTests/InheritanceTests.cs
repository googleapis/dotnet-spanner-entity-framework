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

using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.InheritanceTests.Model;
using Microsoft.EntityFrameworkCore;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.InheritanceTests
{
    public class InheritanceTests : IClassFixture<InheritanceTestFixture>
    {
        private readonly InheritanceTestFixture _fixture;

        public InheritanceTests(InheritanceTestFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task CanUseInheritance()
        {
            var stageWorkerId = _fixture.RandomLong();
            var singerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            using (var db = new TestSpannerInheritanceDbContext(_fixture.DatabaseName))
            {
                var singer = new Singer
                {
                    PersonId = singerId,
                    Name = "Alice Mendelson",
                    StageName = "AliceM",
                };
                db.Singers.Add(singer);
                db.StageWorkers.Add(new StageWorker
                {
                    PersonId = stageWorkerId,
                    Name = "Pete Henderson",
                    WorksFor = singer,
                });
                db.Albums.Add(new Album
                {
                    AlbumId = albumId,
                    Singer = singer,
                    Title = "Some title",
                });
                await db.SaveChangesAsync();
            }

            using (var db = new TestSpannerInheritanceDbContext(_fixture.DatabaseName))
            {
                var stageWorker = await db.StageWorkers.FindAsync(stageWorkerId);
                var singer = await db.Singers.FindAsync(singerId);
                var album = await db.Albums.FindAsync(albumId);

                Assert.Equal("Alice Mendelson", singer.Name);
                Assert.Equal("AliceM", singer.StageName);
                Assert.Equal("Pete Henderson", stageWorker.Name);
                Assert.Same(singer, stageWorker.WorksFor);
                // Verify that a collection on the concrete subclass can be lazily fetched.
                Assert.True(singer.Albums.Contains(album));

                // Verify that we can get all persons regardless of the concrete type.
                var persons = await db.Persons.OrderBy(person => person.Name).ToListAsync();
                Assert.Collection(persons,
                    person => Assert.Equal("Alice Mendelson", person.Name),
                    person => Assert.Equal("Pete Henderson", person.Name)
                );
            }
        }
    }
}
