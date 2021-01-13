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
                ColBoolArray = new List<bool> { true, false, true },
                ColBytes = new byte[] { 1, 2, 3 },
                ColBytesMax = Encoding.UTF8.GetBytes("This is a long string"),
                ColBytesArray = new List<byte[]> { new byte[] { 3, 2, 1 }, new byte[] { }, new byte[] { 4, 5, 6 } },
                ColBytesMaxArray = new List<byte[]> { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2"), Encoding.UTF8.GetBytes("string 3") },
                ColDate = new SpannerDate(2020, 12, 28),
                ColDateArray = new List<SpannerDate> { new SpannerDate(2020, 12, 28), new SpannerDate(2010, 1, 1), today },
                ColFloat64 = 3.14D,
                ColFloat64Array = new List<double> { 3.14D, 6.626D },
                ColInt64 = id1,
                ColInt64Array = new List<long> { 1L, 2L, 4L, 8L },
                ColNumeric = (SpannerNumeric?)3.14m,
                ColNumericArray = new List<SpannerNumeric> { (SpannerNumeric)3.14m, (SpannerNumeric)6.626m },
                ColString = "some string",
                ColStringArray = new List<string> { "string1", "string2", "string3" },
                ColStringMax = "some longer string",
                ColStringMaxArray = new List<string> { "longer string1", "longer string2", "longer string3" },
                ColTimestamp = new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288),
                ColTimestampArray = new List<DateTime> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now },
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
