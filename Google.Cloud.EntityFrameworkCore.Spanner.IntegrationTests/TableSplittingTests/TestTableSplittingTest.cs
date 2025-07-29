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

using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.TableSplittingTests
{
    public class TestTableSplittingTest : IClassFixture<TableSplittingTestFixture>
    {
        private readonly TableSplittingTestFixture _fixture;

        public TestTableSplittingTest(TableSplittingTestFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task ShouldGenerateOneTable()
        {
            using var connection = _fixture.GetConnection();

            var cmd = connection.CreateSelectCommand(
                "SELECT COUNT(*) " +
                "FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_CATALOG='' AND TABLE_SCHEMA='' AND TABLE_NAME NOT IN('EFMigrationsHistory', 'EFMigrationsLock')");

            using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(1, reader.GetInt64(0));
            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task ShouldNotGenerateAForeignKey()
        {
            using var connection = _fixture.GetConnection();
            var cmd = connection.CreateSelectCommand(
                "SELECT COUNT(*) " +
                "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS");
            using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(0, reader.GetInt64(0));
            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task CanInsertOrders()
        {
            using var db = new TestTableSplittingDbContext(_fixture.DatabaseName);
            var orderId = _fixture.RandomLong();
            var order = new Models.Order
            {
                Id = orderId.GetHashCode(),
                Status = "Processing",
                OrderDetail = new Models.OrderDetail
                {
                    Id = orderId.GetHashCode(),
                    Status = "Processing",
                    ShippingAddress = "Some address",
                    BillingAddress = "Some other address",
                },
            };
            db.Orders.Add(order);
            var rowCount = await db.SaveChangesAsync();
            Assert.Equal(1, rowCount);
        }
    }
}
