using Google.Cloud.Spanner.Data;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    [Collection(nameof(SpannerMigrationFixture))]
    public class SpannerMigrationTest
    {
        private readonly SpannerMigrationFixture _fixture;

        public SpannerMigrationTest(SpannerMigrationFixture fixture) => _fixture = fixture;

        [Fact]
        public async void AllTablesAreGenerated()
        {
            using var connection = _fixture.GetConnection();
            var tableNames = new string[] { "Products", "Brands", "Orders", "OrderDetails" };
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
    }
}
