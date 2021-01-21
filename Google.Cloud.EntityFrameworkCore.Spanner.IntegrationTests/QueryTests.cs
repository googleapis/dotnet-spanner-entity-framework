// Copyright 2021 Google LLC
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     https://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    public class QueryTests : IClassFixture<SpannerSampleFixture>
    {
        private readonly SpannerSampleFixture _fixture;

        public QueryTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task CanFilterOnProperty()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.Add(new Singers { SingerId = singerId, FirstName = "Pete", LastName = "Peterson" });
            await db.SaveChangesAsync();

            var singer = await db.Singers
                .Where(s => s.FullName == "Pete Peterson" && s.SingerId == singerId)
                .FirstOrDefaultAsync();
            Assert.NotNull(singer);
            Assert.Equal("Pete Peterson", singer.FullName);
        }

        [Fact]
        public async Task CanOrderByProperty()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            await db.SaveChangesAsync();

            var singers = await db.Singers
                .Where(s => s.SingerId == singerId1 || s.SingerId == singerId2)
                .OrderBy(s => s.LastName)
                .ToListAsync();
            Assert.Collection(singers,
                s => Assert.Equal("Zeke Allison", s.FullName),
                s => Assert.Equal("Pete Peterson", s.FullName)
            );
        }

        [Fact]
        public async Task CanIncludeProperty()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            db.Albums.AddRange(
                new Albums { AlbumId = _fixture.RandomLong(), Title = "Album 1", SingerId = singerId1 },
                new Albums { AlbumId = _fixture.RandomLong(), Title = "Album 2", SingerId = singerId1 },
                new Albums { AlbumId = _fixture.RandomLong(), Title = "Album 3", SingerId = singerId2 },
                new Albums { AlbumId = _fixture.RandomLong(), Title = "Album 4", SingerId = singerId2 },
                new Albums { AlbumId = _fixture.RandomLong(), Title = "Album 5", SingerId = singerId2 }
            );
            await db.SaveChangesAsync();

            var albums = await db.Albums
                .Include(a => a.Singer)
                .Where(a => a.Singer.LastName == "Allison")
                .OrderBy(a => a.Title)
                .ToListAsync();
            Assert.Collection(albums,
                a => Assert.Equal("Album 3", a.Title),
                a => Assert.Equal("Album 4", a.Title),
                a => Assert.Equal("Album 5", a.Title)
            );
        }

        [Fact]
        public async Task CanUseLimitWithoutOffset()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            await db.SaveChangesAsync();

            var singers = await db.Singers
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .OrderBy(s => s.LastName)
                .Take(1)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Allison", s.LastName));
        }

        [Fact]
        public async Task CanUseLimitWithOffset()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            var singerId3 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" },
                new Singers { SingerId = singerId3, FirstName = "Sandra", LastName = "Ericson" }
            );
            await db.SaveChangesAsync();

            var singers = await db.Singers
                .Where(s => new long[] { singerId1, singerId2, singerId3 }.Contains(s.SingerId))
                .OrderBy(s => s.LastName)
                .Skip(1)
                .Take(1)
                .ToListAsync();

            Assert.Collection(singers,
                s => Assert.Equal("Ericson", s.LastName)
            );
        }

        [Fact]
        public async Task CanUseOffsetWithoutLimit()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            await db.SaveChangesAsync();

            var singers = await db.Singers
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .OrderBy(s => s.LastName)
                .Skip(1)
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Peterson", s.LastName));
        }

        [Fact]
        public async Task CanUseInnerJoin()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            db.Albums.Add(new Albums { AlbumId = _fixture.RandomLong(), Title = "Some Title", SingerId = singerId1 });
            await db.SaveChangesAsync();

            var singers = await db.Singers
                .Join(db.Albums, a => a.SingerId, s => s.SingerId, (s, a) => new { Singer = s, Album = a })
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.Singer.SingerId))
                .ToListAsync();

            Assert.Collection(singers,
                s =>
                {
                    Assert.Equal("Peterson", s.Singer.LastName);
                    Assert.Equal("Some Title", s.Album.Title);
                }
            );
        }

        [Fact]
        public async Task CanUseOuterJoin()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            db.Albums.Add(new Albums { AlbumId = _fixture.RandomLong(), Title = "Some Title", SingerId = singerId1 });
            await db.SaveChangesAsync();

            var singers = await db.Singers
                .GroupJoin(db.Albums, s => s.SingerId, a => a.SingerId, (s, a) => new { Singer = s, Albums = a })
                .SelectMany(
                    s => s.Albums.DefaultIfEmpty(),
                    (s, a) => new { s.Singer, Album = a })
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.Singer.SingerId))
                .OrderBy(s => s.Singer.LastName)
                .ToListAsync();

            Assert.Collection(singers,
                s =>
                {
                    Assert.Equal("Allison", s.Singer.LastName);
                    Assert.Null(s.Album);
                },
                s =>
                {
                    Assert.Equal("Peterson", s.Singer.LastName);
                    Assert.Equal("Some Title", s.Album.Title);
                }
            );
        }

        [Fact]
        public async Task CanUseStringContains()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            await db.SaveChangesAsync();

            var fullName = "Alli";
            var singers = await db.Singers
                .Where(s => s.FullName.Contains(fullName))
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Zeke Allison", s.FullName));
        }

        [Fact]
        public async Task CanUseStringStartsWith()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            await db.SaveChangesAsync();

            var fullName = "Zeke";
            var singers = await db.Singers
                .Where(s => s.FullName.StartsWith(fullName))
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Zeke Allison", s.FullName));
        }

        [Fact]
        public async Task CanUseStringEndsWith()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            await db.SaveChangesAsync();

            var fullName = "Peterson";
            var singers = await db.Singers
                .Where(s => s.FullName.EndsWith(fullName))
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Pete Peterson", s.FullName));
        }

        [Fact]
        public async Task CanUseStringLength()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var minLength = 4;
            var singers = await db.Singers
                .Where(s => s.FirstName.Length > minLength)
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Alice Morrison", s.FullName));
        }

        [Fact]
        public async Task CanUseStringIndexOf()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var name = "Morrison";
            var minIndex = -1;
            var singers = await db.Singers
                .Where(s => s.FullName.IndexOf(name) > minIndex)
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Alice Morrison", s.FullName));
        }

        [Fact]
        public async Task CanUseStringReplace()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var from = "Pete";
            var to = "Peter";
            var name = "Peter Peterrson";
            var singers = await db.Singers
                .Where(s => s.FullName.Replace(from, to) == name)
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .ToListAsync();

            Assert.Collection(singers, s => Assert.Equal("Pete Peterson", s.FullName));
        }

        [Fact]
        public async Task CanUseStringToLower()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var fullNameLowerCase = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.ToLower())
                .FirstOrDefaultAsync();

            Assert.Equal("alice morrison", fullNameLowerCase);
        }

        [Fact]
        public async Task CanUseStringToUpper()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var fullNameLowerCase = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.ToUpper())
                .FirstOrDefaultAsync();

            Assert.Equal("ALICE MORRISON", fullNameLowerCase);
        }

        [Fact]
        public async Task CanUseStringSubstring()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var lastNameFromFullName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.Substring(6))
                .FirstOrDefaultAsync();

            Assert.Equal("Morrison", lastNameFromFullName);
        }

        [Fact]
        public async Task CanUseStringSubstringWithLength()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var lastNameFromFullName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.Substring(6, 3))
                .FirstOrDefaultAsync();

            Assert.Equal("Mor", lastNameFromFullName);
        }

        [Fact]
        public async Task CanUseStringTrimStart()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "   Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var trimmedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.TrimStart())
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringTrimStartWithArgument()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "\t\t\tAlice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var trimmedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.TrimStart('\t'))
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringTrimEnd()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison   " }
            );
            await db.SaveChangesAsync();

            var trimmedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.TrimEnd())
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringTrimEndWithArgument()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison\t\t\t" }
            );
            await db.SaveChangesAsync();

            var trimmedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.TrimEnd('\t'))
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringTrim()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "   Alice", LastName = "Morrison   " }
            );
            await db.SaveChangesAsync();

            var trimmedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.Trim())
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringTrimWithArgument()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "\t\t\tAlice", LastName = "Morrison\t\t\t" }
            );
            await db.SaveChangesAsync();

            var trimmedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.Trim('\t'))
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", trimmedName);
        }

        [Fact]
        public async Task CanUseStringConcat()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var calculatedFullName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FirstName + " " + s.LastName)
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison", calculatedFullName);
        }

        [Fact]
        public async Task CanUseStringPadLeft()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var paddedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.PadLeft(20))
                .FirstOrDefaultAsync();

            Assert.Equal("      Alice Morrison", paddedName);
        }

        [Fact]
        public async Task CanUseStringPadLeftWithChar()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var paddedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.PadLeft(20, '$'))
                .FirstOrDefaultAsync();

            Assert.Equal("$$$$$$Alice Morrison", paddedName);
        }

        [Fact]
        public async Task CanUseStringPadRight()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var paddedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.PadRight(20))
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison      ", paddedName);
        }

        [Fact]
        public async Task CanUseStringPadRightWithChar()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var paddedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => s.FullName.PadRight(20, '$'))
                .FirstOrDefaultAsync();

            Assert.Equal("Alice Morrison$$$$$$", paddedName);
        }

        [Fact]
        public async Task CanUseStringFormat()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId, FirstName = "Alice", LastName = "Morrison", BirthDate = new SpannerDate(1973, 10, 9)}
            );
            await db.SaveChangesAsync();

            var formattedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => string.Format("String without formatting"))
                .FirstOrDefaultAsync();
            Assert.Equal("String without formatting", formattedName);

            formattedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => string.Format("%s", s.FullName))
                .FirstOrDefaultAsync();
            Assert.Equal("Alice Morrison", formattedName);

            formattedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => string.Format("%025d: %s", s.SingerId, s.FullName))
                .FirstOrDefaultAsync();
            Assert.Equal($"{singerId.ToString().PadLeft(25, '0')}: Alice Morrison", formattedName);

            formattedName = await db.Singers
                .Where(s => new long[] { singerId }.Contains(s.SingerId))
                .Select(s => string.Format("%025d: %s, born on %t", s.SingerId, s.FullName, s.BirthDate))
                .FirstOrDefaultAsync();
            Assert.Equal($"{singerId.ToString().PadLeft(25, '0')}: Alice Morrison, born on {new SpannerDate(1973, 10, 9)}", formattedName);
        }

        [Fact]
        public async Task CanUseRegexIsMatch()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var pattern = ".*Peterson";
            var regex = new Regex(pattern);
            var singers = await db.Singers
                .Where(s => regex.IsMatch(s.FullName))
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .ToListAsync();
            Assert.Collection(singers, s => Assert.Equal("Pete Peterson", s.FullName));

            singers = await db.Singers
                .Where(s => Regex.IsMatch(s.FullName, pattern))
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .ToListAsync();
            Assert.Collection(singers, s => Assert.Equal("Pete Peterson", s.FullName));
        }

        [Fact]
        public async Task CanUseRegexReplace()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Alice", LastName = "Morrison" }
            );
            await db.SaveChangesAsync();

            var replacement = "Allison";
            var pattern = "Al.*";
            var regex = new Regex(pattern);
            var firstNames = await db.Singers
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .OrderBy(s => s.LastName)
                .Select(s => regex.Replace(s.FirstName, replacement))
                .ToListAsync();
            Assert.Collection(firstNames,
                s => Assert.Equal("Allison", s),
                s => Assert.Equal("Pete", s)
            );

            firstNames = await db.Singers
                .Where(s => new long[] { singerId1, singerId2 }.Contains(s.SingerId))
                .OrderBy(s => s.LastName)
                .Select(s => Regex.Replace(s.FirstName, pattern, replacement))
                .ToListAsync();
            Assert.Collection(firstNames,
                s => Assert.Equal("Allison", s),
                s => Assert.Equal("Pete", s)
            );
        }

        [Fact]
        public async Task CanQueryRawSqlWithParameters()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var singerId2 = _fixture.RandomLong();
            db.Singers.AddRange(
                new Singers { SingerId = singerId1, FirstName = "Pete", LastName = "Peterson" },
                new Singers { SingerId = singerId2, FirstName = "Zeke", LastName = "Allison" }
            );
            await db.SaveChangesAsync();

            var singers = await db.Singers
                .FromSqlRaw("SELECT * FROM Singers WHERE SingerId IN UNNEST(@id)", new SpannerParameter("id", SpannerDbType.ArrayOf(SpannerDbType.Int64), new List<long> { singerId1, singerId2 }))
                .OrderBy(s => s.LastName)
                .ToListAsync();
            Assert.Collection(singers,
                s => Assert.Equal("Allison", s.LastName),
                s => Assert.Equal("Peterson", s.LastName)
            );
        }

        [Fact]
        public async Task CanInsertSingerUsingRawSql()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var singerId1 = _fixture.RandomLong();
            var firstName1 = "Pete";
            var lastName1 = "Peterson";
            var updateCount1 = await db.Database.ExecuteSqlRawAsync(
                "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (@id, @firstName, @lastName)",
                new SpannerParameter("id", SpannerDbType.Int64, singerId1),
                new SpannerParameter("firstName", SpannerDbType.String, firstName1),
                new SpannerParameter("lastName", SpannerDbType.String, lastName1)
            );

            var singerId2 = _fixture.RandomLong();
            var firstName2 = "Zeke";
            var lastName2 = "Allison";
            var updateCount2 = await db.Database.ExecuteSqlInterpolatedAsync(
                $"INSERT INTO Singers (SingerId, FirstName, LastName) VALUES ({singerId2}, {firstName2}, {lastName2})"
            );

            var singerId3 = _fixture.RandomLong();
            var firstName3 = "Luke";
            var lastName3 = "Harrison";
            var updateCount3 = await db.Database.ExecuteSqlRawAsync(
                "INSERT INTO Singers (SingerId, FirstName, LastName) VALUES ({0}, {1}, {2})",
                singerId3, firstName3, lastName3
            );

            Assert.Equal(1, updateCount1);
            Assert.Equal(1, updateCount2);
            Assert.Equal(1, updateCount3);
            var singers = await db.Singers
                .FromSqlRaw("SELECT * FROM Singers WHERE SingerId IN UNNEST(@id)", new SpannerParameter("id", SpannerDbType.ArrayOf(SpannerDbType.Int64), new List<long> { singerId1, singerId2, singerId3 }))
                .OrderBy(s => s.LastName)
                .ToListAsync();
            Assert.Collection(singers,
                s => Assert.Equal("Allison", s.LastName),
                s => Assert.Equal("Harrison", s.LastName),
                s => Assert.Equal("Peterson", s.LastName)
            );
        }

        [Fact]
        public async Task CanInsertRowWithAllColumnTypesUsingRawSql()
        {
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            var id1 = _fixture.RandomLong();
            var today = SpannerDate.FromDateTime(DateTime.SpecifyKind(DateTime.Today, DateTimeKind.Unspecified));
            var now = DateTime.UtcNow;

            var row = new TableWithAllColumnTypes
            {
                ColBool = true,
                ColBoolArray = new List<bool?> { true, false, true },
                ColBytes = new byte[] { 1, 2, 3 },
                ColBytesMax = Encoding.UTF8.GetBytes("This is a long string"),
                ColBytesArray = new List<byte[]> { new byte[] { 3, 2, 1 }, new byte[] { }, new byte[] { 4, 5, 6 } },
                ColBytesMaxArray = new List<byte[]> { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2"), Encoding.UTF8.GetBytes("string 3") },
                ColDate = new SpannerDate(2020, 12, 28),
                ColDateArray = new List<SpannerDate?> { new SpannerDate(2020, 12, 28), new SpannerDate(2010, 1, 1), today },
                ColFloat64 = 3.14D,
                ColFloat64Array = new List<double?> { 3.14D, 6.626D },
                ColInt64 = id1,
                ColInt64Array = new List<long?> { 1L, 2L, 4L, 8L },
                ColNumeric = (SpannerNumeric?)3.14m,
                ColNumericArray = new List<SpannerNumeric?> { (SpannerNumeric)3.14m, (SpannerNumeric)6.626m },
                ColString = "some string",
                ColStringArray = new List<string> { "string1", "string2", "string3" },
                ColStringMax = "some longer string",
                ColStringMaxArray = new List<string> { "longer string1", "longer string2", "longer string3" },
                ColTimestamp = new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288),
                ColTimestampArray = new List<DateTime?> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now },
            };
            var updateCount1 = await db.Database.ExecuteSqlRawAsync(
                @"INSERT INTO TableWithAllColumnTypes 
                              (ColBool, ColBoolArray, ColBytes, ColBytesMax, ColBytesArray, ColBytesMaxArray,
                               ColDate, ColDateArray, ColFloat64, ColFloat64Array, ColInt64, ColInt64Array,
                               ColNumeric, ColNumericArray, ColString, ColStringArray, ColStringMax, ColStringMaxArray,
                               ColTimestamp, ColTimestampArray)
                              VALUES
                              (@ColBool, @ColBoolArray, @ColBytes, @ColBytesMax, @ColBytesArray, @ColBytesMaxArray,
                               @ColDate, @ColDateArray, @ColFloat64, @ColFloat64Array, @ColInt64, @ColInt64Array,
                               @ColNumeric, @ColNumericArray, @ColString, @ColStringArray, @ColStringMax, @ColStringMaxArray,
                               @ColTimestamp, @ColTimestampArray)",
                new SpannerParameter("ColBool", SpannerDbType.Bool, row.ColBool),
                new SpannerParameter("ColBoolArray", SpannerDbType.ArrayOf(SpannerDbType.Bool), row.ColBoolArray),
                new SpannerParameter("ColBytes", SpannerDbType.Bytes, row.ColBytes),
                new SpannerParameter("ColBytesMax", SpannerDbType.Bytes, row.ColBytesMax),
                new SpannerParameter("ColBytesArray", SpannerDbType.ArrayOf(SpannerDbType.Bytes), row.ColBytesArray),
                new SpannerParameter("ColBytesMaxArray", SpannerDbType.ArrayOf(SpannerDbType.Bytes), row.ColBytesMaxArray),
                new SpannerParameter("ColDate", SpannerDbType.Date, row.ColDate),
                new SpannerParameter("ColDateArray", SpannerDbType.ArrayOf(SpannerDbType.Date), row.ColDateArray),
                new SpannerParameter("ColFloat64", SpannerDbType.Float64, row.ColFloat64),
                new SpannerParameter("ColFloat64Array", SpannerDbType.ArrayOf(SpannerDbType.Float64), row.ColFloat64Array),
                new SpannerParameter("ColInt64", SpannerDbType.Int64, row.ColInt64),
                new SpannerParameter("ColInt64Array", SpannerDbType.ArrayOf(SpannerDbType.Int64), row.ColInt64Array),
                new SpannerParameter("ColNumeric", SpannerDbType.Numeric, row.ColNumeric),
                new SpannerParameter("ColNumericArray", SpannerDbType.ArrayOf(SpannerDbType.Numeric), row.ColNumericArray),
                new SpannerParameter("ColString", SpannerDbType.String, row.ColString),
                new SpannerParameter("ColStringArray", SpannerDbType.ArrayOf(SpannerDbType.String), row.ColStringArray),
                new SpannerParameter("ColStringMax", SpannerDbType.String, row.ColStringMax),
                new SpannerParameter("ColStringMaxArray", SpannerDbType.ArrayOf(SpannerDbType.String), row.ColStringMaxArray),
                new SpannerParameter("ColTimestamp", SpannerDbType.Timestamp, row.ColTimestamp),
                new SpannerParameter("ColTimestampArray", SpannerDbType.ArrayOf(SpannerDbType.Timestamp), row.ColTimestampArray)
            );
            Assert.Equal(1, updateCount1);

            var id2 = _fixture.RandomLong();
            row.ColInt64 = id2;
            var updateCount2 = await db.Database.ExecuteSqlInterpolatedAsync(
                @$"INSERT INTO TableWithAllColumnTypes 
                              (ColBool, ColBoolArray, ColBytes, ColBytesMax, ColBytesArray, ColBytesMaxArray,
                               ColDate, ColDateArray, ColFloat64, ColFloat64Array, ColInt64, ColInt64Array,
                               ColNumeric, ColNumericArray, ColString, ColStringArray, ColStringMax, ColStringMaxArray,
                               ColTimestamp, ColTimestampArray)
                              VALUES
                              ({row.ColBool}, {row.ColBoolArray}, {row.ColBytes}, {row.ColBytesMax}, {row.ColBytesArray}, {row.ColBytesMaxArray},
                               {row.ColDate}, {row.ColDateArray}, {row.ColFloat64}, {row.ColFloat64Array}, {row.ColInt64}, {row.ColInt64Array},
                               {row.ColNumeric}, {row.ColNumericArray}, {row.ColString}, {row.ColStringArray}, {row.ColStringMax}, {row.ColStringMaxArray},
                               {row.ColTimestamp}, {row.ColTimestampArray})"
            );
            Assert.Equal(1, updateCount2);

            var id3 = _fixture.RandomLong();
            row.ColInt64 = id3;
            var updateCount3 = await db.Database.ExecuteSqlRawAsync(
                @"INSERT INTO TableWithAllColumnTypes 
                              (ColBool, ColBoolArray, ColBytes, ColBytesMax, ColBytesArray, ColBytesMaxArray,
                               ColDate, ColDateArray, ColFloat64, ColFloat64Array, ColInt64, ColInt64Array,
                               ColNumeric, ColNumericArray, ColString, ColStringArray, ColStringMax, ColStringMaxArray,
                               ColTimestamp, ColTimestampArray)
                              VALUES
                              ({0}, {1}, {2}, {3}, {4}, {5},
                               {6}, {7}, {8}, {9}, {10}, {11},
                               {12}, {13}, {14}, {15}, {16}, {17},
                               {18}, {19})",
                               row.ColBool, row.ColBoolArray, row.ColBytes, row.ColBytesMax, row.ColBytesArray, row.ColBytesMaxArray,
                               row.ColDate, row.ColDateArray, row.ColFloat64, row.ColFloat64Array, row.ColInt64, row.ColInt64Array,
                               row.ColNumeric, row.ColNumericArray, row.ColString, row.ColStringArray, row.ColStringMax, row.ColStringMaxArray,
                               row.ColTimestamp, row.ColTimestampArray
            );
            Assert.Equal(1, updateCount3);

            var rows = await db.TableWithAllColumnTypes
                .FromSqlRaw("SELECT * FROM TableWithAllColumnTypes WHERE ColInt64 IN UNNEST(@id)", new SpannerParameter("id", SpannerDbType.ArrayOf(SpannerDbType.Int64), new List<long> { id1, id2, id3 }))
                .OrderBy(s => s.ColString)
                .ToListAsync();
            Assert.Collection(rows,
                row => Assert.NotNull(row.ColDateArray),
                row => Assert.NotNull(row.ColDateArray),
                row => Assert.NotNull(row.ColDateArray)
            );
        }
    }
}
