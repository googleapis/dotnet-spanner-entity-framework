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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
    public class TestEntity
    {
        public long Id { get; set; }

        public byte ByteCol { get; set; }

        public decimal DecimalCol { get; set; }

        public float FloatCol { get; set; }
    }

    internal class TypeConversionDbContext : DbContext
    {
        private readonly string _connectionString;

        internal TypeConversionDbContext(string connectionString) : base()
        {
            _connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder
                    .UseSpanner(_connectionString, _ => SpannerModelValidationConnectionProvider.Instance.EnableDatabaseModelValidation(false), ChannelCredentials.Insecure)
                    .UseLazyLoadingProxies();
            }
        }

        public virtual DbSet<TestEntity> TestEntities { get; set; }
    }

    public class TypeConversionTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public TypeConversionTests(SpannerMockServerFixture service)
        {
            _fixture = service;
            service.SpannerMock.Reset();
        }

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        [Fact]
        public async Task TestEntity_ConvertValuesWithoutPrecisionLossOrOverflow_Succeeds()
        {
            var sql = $"SELECT `t`.`Id`, `t`.`ByteCol`, `t`.`DecimalCol`, `t`.`FloatCol`" +
                $"{Environment.NewLine}FROM `TestEntities` AS `t`{Environment.NewLine}" +
                $"WHERE `t`.`Id` = @__p_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.Type, string>>
                {
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Int64 }, "Id"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Int64 }, "ByteCol"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Numeric }, "DecimalCol"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Float64 }, "FloatCol"),
                },
                new List<object[]>
                {
                    new object[] { 1L, 1L, "3.14", 1.0d },
                }
            ));

            using var db = new TypeConversionDbContext(ConnectionString);
            var row = await db.TestEntities.FindAsync(1L);
            Assert.Equal(1L, row.Id);
            Assert.Equal((byte)1, row.ByteCol);
            Assert.Equal(SpannerNumeric.Parse("3.14"), SpannerNumeric.FromDecimal(row.DecimalCol, LossOfPrecisionHandling.Truncate));
            Assert.Equal(1.0d, row.FloatCol);
        }

        [Fact]
        public async Task TestEntity_ConvertValuesWithByteOverflow_Fails()
        {
            var sql = $"SELECT `t`.`Id`, `t`.`ByteCol`, `t`.`DecimalCol`, `t`.`FloatCol`" +
                $"{Environment.NewLine}FROM `TestEntities` AS `t`{Environment.NewLine}" +
                $"WHERE `t`.`Id` = @__p_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.Type, string>>
                {
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Int64 }, "Id"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Int64 }, "ByteCol"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Numeric }, "DecimalCol"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Float64 }, "FloatCol"),
                },
                new List<object[]>
                {
                    new object[] { 1L, 256L, "3.14", 1.0d },
                }
            ));

            using var db = new TypeConversionDbContext(ConnectionString);
            await Assert.ThrowsAsync<OverflowException>(() => db.TestEntities.FindAsync(1L).AsTask());
        }

        [Fact]
        public async Task TestEntity_ConvertValuesWithFloatOverflow_Succeeds()
        {
            var sql = $"SELECT `t`.`Id`, `t`.`ByteCol`, `t`.`DecimalCol`, `t`.`FloatCol`" +
                $"{Environment.NewLine}FROM `TestEntities` AS `t`{Environment.NewLine}" +
                $"WHERE `t`.`Id` = @__p_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.Type, string>>
                {
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Int64 }, "Id"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Int64 }, "ByteCol"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Numeric }, "DecimalCol"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Float64 }, "FloatCol"),
                },
                new List<object[]>
                {
                    new object[] { 1L, 1L, "3.14", float.MaxValue * 2d },
                }
            ));

            using var db = new TypeConversionDbContext(ConnectionString);
            var row = await db.TestEntities.FindAsync(1L);
            Assert.NotNull(row);
            Assert.Equal(float.PositiveInfinity, row.FloatCol);
        }

        [Fact]
        public async Task TestEntity_ConvertValuesWithDecimalOverflow_Fails()
        {
            var sql = $"SELECT `t`.`Id`, `t`.`ByteCol`, `t`.`DecimalCol`, `t`.`FloatCol`" +
                $"{Environment.NewLine}FROM `TestEntities` AS `t`{Environment.NewLine}" +
                $"WHERE `t`.`Id` = @__p_0{Environment.NewLine}LIMIT 1";
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.Type, string>>
                {
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Int64 }, "Id"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Int64 }, "ByteCol"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Numeric }, "DecimalCol"),
                    new Tuple<V1.Type, string>(new V1.Type { Code = V1.TypeCode.Float64 }, "FloatCol"),
                },
                new List<object[]>
                {
                    new object[] { 1L, 1L, $"99999999999999999999999999999", 1d },
                }
            ));

            using var db = new TypeConversionDbContext(ConnectionString);
            await Assert.ThrowsAsync<OverflowException>(() => db.TestEntities.FindAsync(1L).AsTask());
        }
    }
}
