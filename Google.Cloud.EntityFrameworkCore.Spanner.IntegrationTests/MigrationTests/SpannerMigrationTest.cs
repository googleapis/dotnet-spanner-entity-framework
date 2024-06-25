// Copyright 2020 Google LLC
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

using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;
using SpannerDate = Google.Cloud.EntityFrameworkCore.Spanner.Storage.SpannerDate;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.MigrationTests
{
    public class SpannerMigrationTest : IClassFixture<MigrationTestFixture>
    {
        private readonly MigrationTestFixture _fixture;

        public SpannerMigrationTest(MigrationTestFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task AllTablesAreGenerated()
        {
            using var connection = _fixture.GetConnection();
            var tableNames = new [] { "Products", "Categories", "Orders", "OrderDetails", "Articles", "Authors" };
            var tables = new SpannerParameterCollection
            {
                { "tables", SpannerDbType.ArrayOf(SpannerDbType.String), tableNames }
            };
            var cmd = connection.CreateSelectCommand(
                "SELECT COUNT(*) " +
                "FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_CATALOG='' AND TABLE_SCHEMA='' AND TABLE_NAME IN UNNEST (@tables)", tables);

            using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(tableNames.Length, reader.GetInt64(0));
            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task CanInsertUpdateCategories()
        {
            using var context = new TestMigrationDbContext(_fixture.DatabaseName);
            context.Categories.AddRange(new List<Category>
            {
                new Category {
                    CategoryId = 3,
                    CategoryName = "Beverages",
                    CategoryDescription = "Soft drinks, coffees, teas, beers, and ales"
                }, new Category {
                    CategoryId = 4,
                    CategoryName = "Seafood",
                    CategoryDescription = "Seaweed and fish"
                }
            });
            var rowCount = await context.SaveChangesAsync();
            Assert.Equal(2, rowCount);

            // Update category
            var category = await context.Categories.FindAsync(1L);
            Assert.NotNull(category);
            category.CategoryName = "Dairy Products";
            category.CategoryDescription = "Cheeses";
            await context.SaveChangesAsync();

            // Get updated category from db
            category = await context.Categories.FindAsync(1L);
            Assert.NotNull(category);
            Assert.Equal("Dairy Products", category.CategoryName);
            Assert.Equal("Cheeses", category.CategoryDescription);
        }

        [Fact]
        public async Task CanInsertAndUpdateNullValues()
        {
            using var context = new TestMigrationDbContext(_fixture.DatabaseName);

            context.AllColTypes.Add(new AllColType
            {
                Id = 1,
                ColString = "Test String"
            });

            var rowCount = await context.SaveChangesAsync();
            Assert.Equal(1, rowCount);
            var row = await context.AllColTypes.FindAsync(1);
            Assert.NotNull(row);
            Assert.Null(row.ColTimestamp);
            Assert.Null(row.ColShort);
            Assert.Null(row.ColInt);


            // Update from null to non-null.
            row.ColBool = true;
            row.ColBoolArray = new [] { true, false };
            row.ColBytes = Encoding.UTF8.GetBytes("string 1");
            row.ColBytesArray = new [] { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2") };
            row.ColBoolList = new List<bool> { false, true };
            row.ColTimestampList = new List<DateTime> { DateTime.Now, DateTime.Now.AddDays(1) };
            await context.SaveChangesAsync();

            // Retrieve updated row from database
            row = await context.AllColTypes.FindAsync(1);
            Assert.NotNull(row);
            Assert.NotNull(row.ColBool);
            Assert.NotNull(row.ColBoolArray);
            Assert.NotNull(row.ColBytes);
            Assert.NotNull(row.ColBytesArray);
            Assert.NotNull(row.ColBoolList);
            Assert.NotNull(row.ColTimestampList);


            // Update from non-null back to null.
            row.ColBool = null;
            row.ColBoolArray = null;
            row.ColBytes = null;
            row.ColBytesArray = null;
            row.ColBoolList = null;
            row.ColTimestampList = null;
            await context.SaveChangesAsync();

            // Retrieve updated row from database
            row = await context.AllColTypes.FindAsync(1);
            Assert.NotNull(row);
            Assert.Null(row.ColBool);
            Assert.Null(row.ColBoolArray);
            Assert.Null(row.ColBytes);
            Assert.Null(row.ColBytesArray);
            Assert.Null(row.ColBoolList);
            Assert.Null(row.ColTimestampList);
        }

        [Fact]
        public async Task CanInsertAndUpdateRowWithAllDataTypes()
        {
            var now = DateTime.UtcNow;
            var guid = Guid.NewGuid();
            using (var context = new TestMigrationDbContext(_fixture.DatabaseName))
            {
                var row = new AllColType
                {
                    Id = 10,
                    ColBool = true,
                    ColBoolArray = new [] { true, false },
                    ColBoolList = new List<bool> { false, true },
                    ColBytes = Encoding.UTF8.GetBytes("string 1"),
                    ColBytesArray = new [] { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2") },
                    ColBytesList = new List<byte[]> { Encoding.UTF8.GetBytes("string 3"), Encoding.UTF8.GetBytes("string 4") },
                    ColTimestamp = new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288),
                    ColTimestampArray = new [] { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now },
                    ColTimestampList = new List<DateTime> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now },
                    ColDecimal = (SpannerNumeric)10.100m,
                    ColDecimalArray = new [] { (SpannerNumeric)10.1m, (SpannerNumeric)13.5m },
                    ColDecimalList = new List<SpannerNumeric> { (SpannerNumeric)10.1m, (SpannerNumeric)13.5m },
                    ColDouble = 12.01,
                    ColDoubleArray = new [] { 12.01, 12.02 },
                    ColDoubleList = new List<double> { 13.01, 13.02 },
                    ColFloat = 15.999f,
                    ColGuid = guid,
                    ColInt = 10,
                    ColJson = JsonDocument.Parse("{\"key\": \"value\"}"),
                    ColJsonArray = new []{ JsonDocument.Parse("{\"key1\": \"value1\"}"), JsonDocument.Parse("{\"key2\": \"value2\"}") },
                    ColJsonList = new List<JsonDocument> {JsonDocument.Parse("{\"key1\": \"value1\"}"), JsonDocument.Parse("{\"key2\": \"value2\"}")},
                    ColLong = 155,
                    ColLongArray = new long[] { 15, 16 },
                    ColLongList = new List<long> { 20, 25 },
                    ColShort = 10,
                    ColString = "String 1",
                    ColStringArray = new [] { "string1", "string2", "string3" },
                    ColStringList = new List<string> { "string4", "string5" },
                    ColUint = 12,
                    ColDate = new SpannerDate(2021, 1, 1),
                    ColDateArray = new [] { new SpannerDate(2021, 1, 1), new SpannerDate(2021, 1, 2) },
                    ColDateList = new List<SpannerDate> { new (2021, 1, 3), new (2021, 1, 4) },
                    ColByte = 10,
                    ColSbyte = -120,
                    ColULong = 1000000,
                    ColUShort = 2,
                    ColChar = 'a',
                    ASC = "sample string"
                };
                context.AllColTypes.Add(row);
                var rowCount = await context.SaveChangesAsync();
                Assert.Equal(1, rowCount);
            }

            // Get inserted Rows from database.
            using (var context = new TestMigrationDbContext(_fixture.DatabaseName))
            {
                var row = await context.AllColTypes.FindAsync(10);
                Assert.NotNull(row);
                Assert.Equal(10, row.Id);
                Assert.True(row.ColBool);
                Assert.Equal(new [] { true, false }, row.ColBoolArray);
                Assert.Equal(new List<bool> { false, true }, row.ColBoolList);
                Assert.Equal(Encoding.UTF8.GetBytes("string 1"), row.ColBytes);
                Assert.Equal(new [] { Encoding.UTF8.GetBytes("string 1"), Encoding.UTF8.GetBytes("string 2") }, row.ColBytesArray);
                Assert.Equal(new List<byte[]> { Encoding.UTF8.GetBytes("string 3"), Encoding.UTF8.GetBytes("string 4") }, row.ColBytesList);
                Assert.Equal(new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), row.ColTimestamp);
                Assert.Equal(new [] { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now }, row.ColTimestampArray);
                Assert.Equal(new List<DateTime> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(1839288), now }, row.ColTimestampList);
                Assert.Equal((SpannerNumeric)10.100m, row.ColDecimal);
                Assert.Equal(new [] { (SpannerNumeric)10.1m, (SpannerNumeric)13.5m }, row.ColDecimalArray);
                Assert.Equal(new List<SpannerNumeric> { (SpannerNumeric)10.1m, (SpannerNumeric)13.5m }, row.ColDecimalList);
                Assert.Equal(12.01, row.ColDouble);
                Assert.Equal(new [] { 12.01, 12.02 }, row.ColDoubleArray);
                Assert.Equal(new List<double> { 13.01, 13.02 }, row.ColDoubleList);
                Assert.Equal(15.999f, row.ColFloat);
                Assert.Equal(guid, row.ColGuid);
                Assert.Equal(10, row.ColInt);
                if (!SpannerFixtureBase.IsEmulator)
                {
                    Assert.Equal("{\"key\":\"value\"}", row.ColJson.RootElement.ToString());
                    Assert.Equal(new[] { "{\"key1\":\"value1\"}", "{\"key2\":\"value2\"}" },
                        row.ColJsonArray.Select(v => v?.RootElement.ToString()).ToArray());
                    Assert.Equal(new[] { "{\"key1\":\"value1\"}", "{\"key2\":\"value2\"}" },
                        row.ColJsonList.Select(v => v?.RootElement.ToString()).ToList());
                }
                Assert.Equal(155, row.ColLong);
                Assert.Equal(new long[] { 15, 16 }, row.ColLongArray);
                Assert.Equal(new List<long> { 20, 25 }, row.ColLongList);
                Assert.Equal((short)10, row.ColShort);
                Assert.Equal("String 1", row.ColString);
                Assert.Equal(new [] { "string1", "string2", "string3" }, row.ColStringArray);
                Assert.Equal(new List<string> { "string4", "string5" }, row.ColStringList);
                Assert.Equal((uint)12, row.ColUint);
                Assert.Equal(new SpannerDate(2021, 1, 1), row.ColDate);
                Assert.Equal(new [] { new SpannerDate(2021, 1, 1), new SpannerDate(2021, 1, 2) }, row.ColDateArray);
                Assert.Equal(new List<SpannerDate> { new (2021, 1, 3), new (2021, 1, 4) }, row.ColDateList);
                Assert.Equal((byte)10, row.ColByte);
                Assert.Equal((sbyte)-120, row.ColSbyte);
                Assert.Equal((ulong)1000000, row.ColULong);
                Assert.Equal((ushort)2, row.ColUShort);
                Assert.Equal('a', row.ColChar);
                Assert.Equal("sample string", row.ASC);

                // The commit timestamp was automatically set by Cloud Spanner.
                Assert.NotEqual(new DateTime(), row.ColCommitTimestamp);
                // This assumes that the local time does not differ more than 10 minutes with TrueTime.
                if (!SpannerFixtureBase.IsEmulator)
                {
                    Assert.True(Math.Abs(DateTime.UtcNow.Subtract(row.ColCommitTimestamp.GetValueOrDefault()).TotalMinutes) < 10, $"Commit timestamp {row.ColCommitTimestamp} differs with more than 10 minutes from now ({DateTime.UtcNow})");
                }

                // Update rows
                row.ColBool = false;
                row.ColBoolArray = new [] { false, true, false };
                row.ColBoolList = new List<bool> { true, true };
                row.ColBytes = Encoding.UTF8.GetBytes("This string has changed");
                row.ColBytesArray = new [] { Encoding.UTF8.GetBytes("string change 1"), Encoding.UTF8.GetBytes("string change 2") };
                row.ColBytesList = new List<byte[]> { Encoding.UTF8.GetBytes("string change 3"), Encoding.UTF8.GetBytes("string change 4") };
                row.ColTimestamp = new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(5000);
                row.ColTimestampArray = new [] { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(5000), now };
                row.ColTimestampList = new List<DateTime> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(500), now };
                row.ColDecimal = (SpannerNumeric)10.5m;
                row.ColDecimalArray = new [] { (SpannerNumeric)20.1m, (SpannerNumeric)30.5m };
                row.ColDecimalList = new List<SpannerNumeric> { (SpannerNumeric)50m, (SpannerNumeric)15.5m };
                row.ColDouble = 15;
                row.ColDoubleArray = new [] { 15.5 };
                row.ColDoubleList = new List<double> { 30.9 };
                row.ColFloat = 16.52f;
                row.ColInt = 200;
                row.ColJson = JsonDocument.Parse("{\"key\": \"new-value\"}");
                row.ColJsonArray = new[]
                    { JsonDocument.Parse("{\"key1\": \"new-value1\"}"), JsonDocument.Parse("{\"key2\": \"new-value2\"}") };
                row.ColJsonList = new List<JsonDocument>
                    { JsonDocument.Parse("{\"key1\": \"new-value1\"}"), JsonDocument.Parse("{\"key2\": \"new-value2\"}") };
                row.ColLong = 19999;
                row.ColLongArray = new long[] { 17, 18 };
                row.ColLongList = new List<long> { 25, 26 };
                row.ColShort = 1;
                row.ColString = "Updated String 1";
                row.ColStringArray = new [] { "string1 Updated" };
                row.ColStringList = new List<string> { "string2 Updated" };
                row.ColUint = 3;
                row.ColDate = new SpannerDate(2021, 1, 2);
                row.ColDateArray = new [] { new SpannerDate(2021, 1, 3), new SpannerDate(2021, 1, 4) };
                row.ColDateList = new List<SpannerDate> { new (2021, 1, 5), new (2021, 1, 6) };
                row.ColByte = 20;
                row.ColSbyte = -101;
                row.ColULong = 2000000;
                row.ColUShort = 5;
                row.ColChar = 'b';
                row.ASC = "sample string updated";
                await context.SaveChangesAsync();
            }

            // Retrieve Updated Rows
            using (var context = new TestMigrationDbContext(_fixture.DatabaseName))
            {
                var row = await context.AllColTypes.FindAsync(10);
                Assert.NotNull(row);
                Assert.False(row.ColBool);
                Assert.Equal(new [] { false, true, false }, row.ColBoolArray);
                Assert.Equal(new List<bool> { true, true }, row.ColBoolList);
                Assert.Equal(Encoding.UTF8.GetBytes("This string has changed"), row.ColBytes);
                Assert.Equal(new [] { Encoding.UTF8.GetBytes("string change 1"), Encoding.UTF8.GetBytes("string change 2") }, row.ColBytesArray);
                Assert.Equal(new List<byte[]> { Encoding.UTF8.GetBytes("string change 3"), Encoding.UTF8.GetBytes("string change 4") }, row.ColBytesList);
                Assert.Equal(new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(5000), row.ColTimestamp);
                Assert.Equal(new [] { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(5000), now }, row.ColTimestampArray);
                Assert.Equal(new List<DateTime> { new DateTime(2020, 12, 28, 15, 16, 28, 148).AddTicks(500), now }, row.ColTimestampList);
                Assert.Equal((SpannerNumeric)10.5m, row.ColDecimal);
                Assert.Equal(new [] { (SpannerNumeric)20.1m, (SpannerNumeric)30.5m }, row.ColDecimalArray);
                Assert.Equal(new List<SpannerNumeric> { (SpannerNumeric)50m, (SpannerNumeric)15.5m }, row.ColDecimalList);
                Assert.Equal(15, row.ColDouble);
                Assert.Equal(new [] { 15.5 }, row.ColDoubleArray);
                Assert.Equal(new List<double> { 30.9 }, row.ColDoubleList);
                Assert.Equal(16.52f, row.ColFloat);
                Assert.Equal(200, row.ColInt);
                if (!SpannerFixtureBase.IsEmulator)
                {
                    Assert.Equal("{\"key\":\"new-value\"}", row.ColJson.RootElement.ToString());
                    Assert.Equal(new[] { "{\"key1\":\"new-value1\"}", "{\"key2\":\"new-value2\"}" },
                        row.ColJsonArray.Select(v => v?.RootElement.ToString()).ToArray());
                    Assert.Equal(new[] { "{\"key1\":\"new-value1\"}", "{\"key2\":\"new-value2\"}" },
                        row.ColJsonList.Select(v => v?.RootElement.ToString()).ToList());
                }
                Assert.Equal(19999, row.ColLong);
                Assert.Equal(new long[] { 17, 18 }, row.ColLongArray);
                Assert.Equal(new List<long> { 25, 26 }, row.ColLongList);
                Assert.Equal((short)1, row.ColShort);
                Assert.Equal("Updated String 1", row.ColString);
                Assert.Equal(new [] { "string1 Updated" }, row.ColStringArray);
                Assert.Equal(new List<string> { "string2 Updated" }, row.ColStringList);
                Assert.Equal((uint)3, row.ColUint);
                Assert.Equal(new SpannerDate(2021, 1, 2), row.ColDate);
                Assert.Equal(new [] { new SpannerDate(2021, 1, 3), new SpannerDate(2021, 1, 4) }, row.ColDateArray);
                Assert.Equal(new List<SpannerDate> { new (2021, 1, 5), new (2021, 1, 6) }, row.ColDateList);
                Assert.Equal((byte)20, row.ColByte);
                Assert.Equal((sbyte)-101, row.ColSbyte);
                Assert.Equal((ulong)2000000, row.ColULong);
                Assert.Equal((ushort)5, row.ColUShort);
                Assert.Equal('b', row.ColChar);
                Assert.Equal("sample string updated", row.ASC);
            }
        }

        [Fact]
        public async Task CanInsertOrderDetils()
        {
            using var context = new TestMigrationDbContext(_fixture.DatabaseName);
            context.OrderDetails.Add(new OrderDetail
            {
                OrderId = 1,
                ProductId = 1,
                Order = new Order
                {
                    OrderId = 1,
                    OrderDate = DateTime.Now,
                    Freight = 155555.10f,
                    ShipAddress = "Statue of Liberty-New York Access",
                    ShipCountry = "USA",
                    ShipCity = "New York",
                    ShipPostalCode = "10004"
                },
                Discount = 10.0f,
                Product = new Product
                {
                    ProductId = 1,
                    CategoryId = 1,
                    ProductName = "Product 1",
                    Category = new Category
                    {
                        CategoryId = 1,
                        CategoryName = "Grains/Cereals",
                        CategoryDescription = "Breads, crackers, pasta, and cereal"
                    }
                },
                Quantity = 56,
                UnitPrice = 15000
            });
            var rowCount = await context.SaveChangesAsync();
            Assert.Equal(4, rowCount);
        }

        [Fact]
        public async Task CanDeleteData()
        {
            using var context = new TestMigrationDbContext(_fixture.DatabaseName);

            // Insert Category
            var category = new Category
            {
                CategoryId = 99,
                CategoryName = "Confections",
                CategoryDescription = "Desserts, candies, and sweet breads"
            };
            context.Categories.Add(category);

            // Insert Products
            var product = new Product
            {
                ProductId = 99,
                CategoryId = 99,
                ProductName = "Product 99"
            };
            context.Products.Add(product);

            // Insert Order
            var order = new Order
            {
                OrderDate = DateTime.Now,
                OrderId = 99,
                ShipCity = "New York",
                ShipName = "Back yard",
                ShipAddress = "South Street Seaport",
                ShipPostalCode = "	10038",
                ShipCountry = "USA",
                ShippedDate = DateTime.Now.AddDays(10),
                ShipVia = "Transport"
            };
            context.Orders.Add(order);

            // Insert Order Details
            var orderDetail = new OrderDetail
            {
                ProductId = 99,
                OrderId = 99,
                Discount = 10.5f,
                UnitPrice = 1900,
                Quantity = 150
            };
            context.OrderDetails.Add(orderDetail);

            // Insert AllColType
            var allColType = new AllColType
            {
                Id = 99
            };
            context.AllColTypes.Add(allColType);
            await context.SaveChangesAsync();


            // Delete Data
            context.Categories.Remove(category);
            context.Products.Remove(product);
            context.Orders.Remove(order);
            context.OrderDetails.Remove(orderDetail);
            context.AllColTypes.Remove(allColType);
            await context.SaveChangesAsync();

            // Verify that all rows were deleted.
            Assert.Null(await context.Categories.FindAsync(category.CategoryId));
            Assert.Null(await context.Products.FindAsync(product.ProductId));
            Assert.Null(await context.Orders.FindAsync(order.OrderId));
            Assert.Null(await context.OrderDetails.FindAsync(orderDetail.OrderId, orderDetail.ProductId));
            Assert.Null(await context.AllColTypes.FindAsync(allColType.Id));
        }

        [Fact]
        public void ShouldThrowLengthValidationException()
        {
            using var context = new TestMigrationDbContext(_fixture.DatabaseName);
            context.Products.Add(new Product
            {
                ProductId = 9,
                Category = new Category
                {
                    CategoryId = 9,
                    CategoryName = "Soft Drink",
                },
                ProductName = "this is too long string should throw length validation error " +
                "this is too long string should throw length validation error"
            });

            Assert.Throws<SpannerException>(() => context.SaveChanges());
        }

        [Fact]
        public void ShouldThrowRequiredFieldValidationException()
        {
            using var context = new TestMigrationDbContext(_fixture.DatabaseName);
            context.Products.Add(new Product
            {
                ProductId = 9,
                Category = new Category
                {
                    CategoryId = 9,
                    CategoryName = "Soft Drink",
                }
            });

            Assert.Throws<SpannerException>(() => context.SaveChanges());
        }

        [Fact]
        public async Task CanInsertAndDeleteInterleaveOnDeleteCascade()
        {
            using (var context = new TestMigrationDbContext(_fixture.DatabaseName))
            {
                var article = new Article
                {
                    ArticleId = 1,
                    Author = new Author
                    {
                        AuthorId = 4,
                        FirstName = "Calvin",
                        LastName = "Saunders"
                    },
                    ArticleTitle = "Research on Resource Reports",
                    ArticleContent = "This is simple content on resource report research.",
                    PublishDate = new DateTime(2020, 12, 1)
                };
                context.Articles.Add(article);
                var rowCount = await context.SaveChangesAsync();
                Assert.Equal(2, rowCount);
            }

            using (var context = new TestMigrationDbContext(_fixture.DatabaseName))
            {
                // Delete Author Should delete the Article's as well.
                var author = new Author
                {
                    AuthorId = 3,
                    FirstName = "Calvin",
                    LastName = "Saunders"
                };

                context.Authors.Remove(author);
                await context.SaveChangesAsync();

                // Find Article with deleted Author
                var article = context.Articles.FirstOrDefault(c => c.ArticleId == 1 && c.AuthorId == 3);
                Assert.Null(article);
            }
        }

        [Fact]
        public void ShouldThrowInterleaveTableOnInsert()
        {
            using var context = new TestMigrationDbContext(_fixture.DatabaseName);
            var article = new Article
            {
                ArticleId = 1,
                AuthorId = 9999,
                ArticleTitle = "Research on Perspectives",
                ArticleContent = "This is simple content on Perspectives research.",
                PublishDate = new DateTime(2020, 12, 1)
            };
            context.Articles.Add(article);
            Assert.Throws<SpannerException>(() => context.SaveChanges());
        }

        [Fact]
        public async Task ComputedColumn()
        {
            using var context = new TestMigrationDbContext(_fixture.DatabaseName);
            using var transaction = await context.Database.BeginTransactionAsync();
            var author = new Author
            {
                AuthorId = 10,
                FirstName = "Loren",
                LastName = "Ritchie"
            };
            context.Authors.Add(author);
            await context.SaveChangesAsync();
            await transaction.CommitAsync();

            author = await context.Authors.FindAsync(10L);
            Assert.NotNull(author);
            Assert.Equal("Loren Ritchie", author.FullName);
        }

        [Fact]
        public async Task CanSeedData()
        {
            using var context = new TestMigrationDbContext(_fixture.DatabaseName);
            var authors = await context.Authors.Where(c => c.AuthorId == 1 || c.AuthorId == 2).ToListAsync();
            if (_fixture.Database.Fresh)
            {
                Assert.Collection(authors,
                    s => Assert.Equal("Belinda Stiles", s.FullName),
                    s => Assert.Equal("Kelly Houser", s.FullName));
            }
            else
            {
                Assert.Empty(authors);
            }
        }
    }
}
