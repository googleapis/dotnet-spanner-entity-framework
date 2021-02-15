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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Storage
{
#pragma warning disable EF1001 // Internal EF Core API usage.
    public class SpannerTypeMapperTest : RelationalTypeMapperTestBase
    {
        [Fact]
        public void Does_short_mapping()
        {
            Assert.Equal("INT64", GetTypeMapping(typeof(short)).StoreType);
        }

        [Fact]
        public void Does_int_mapping()
        {
            Assert.Equal("INT64", GetTypeMapping(typeof(int)).StoreType);
        }

        [Fact]
        public void Does_long_mapping()
        {
            Assert.Equal("INT64", GetTypeMapping(typeof(long)).StoreType);
        }

        [Fact]
        public void Does_decimal_mapping()
        {
            Assert.Equal("NUMERIC", GetTypeMapping(typeof(decimal)).StoreType);
        }

        [Fact]
        public void Does_spanner_numeric_mapping()
        {
            Assert.Equal("NUMERIC", GetTypeMapping(typeof(SpannerNumeric)).StoreType);
        }

        [Fact]
        public void Does_uint_mapping()
        {
            Assert.Equal("INT64", GetTypeMapping(typeof(uint)).StoreType);
        }

        [Fact]
        public void Does_bool_mapping()
        {
            Assert.Equal("BOOL", GetTypeMapping(typeof(bool)).StoreType);
        }

        [Fact]
        public void Does_spanner_date_mapping()
        {
            Assert.Equal("DATE", GetTypeMapping(typeof(SpannerDate)).StoreType);
        }

        [Fact]
        public void Does_spanner_timestamp_mapping()
        {
            Assert.Equal("TIMESTAMP", GetTypeMapping(typeof(DateTime)).StoreType);
        }

        [Fact]
        public void Does_float_mapping()
        {
            Assert.Equal("FLOAT64", GetTypeMapping(typeof(float)).StoreType);
        }

        [Fact]
        public void Does_double_mapping()
        {
            Assert.Equal("FLOAT64", GetTypeMapping(typeof(double)).StoreType);
        }

        [Fact]
        public void Does_string_default_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(string));
            // Migration Sql Generator will replace `STRING` by `STRING(MAX)`
            Assert.Equal("STRING", typeMapping.StoreType);
        }

        [Fact]
        public void Does_string_with_fix_length_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(string), null, 100);
            Assert.Equal("STRING(100)", typeMapping.StoreType);
            Assert.Equal(100, typeMapping.Size);
        }

        [Fact]
        public void Does_guid_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(Guid));
            Assert.Equal("STRING(36)", typeMapping.StoreType);
            Assert.Equal(36, typeMapping.Size);
        }

        [Fact]
        public void Does_byte_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(byte));
            Assert.Equal("INT64", typeMapping.StoreType);
        }

        [Fact]
        public void Does_sbyte_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(sbyte));
            Assert.Equal("INT64", typeMapping.StoreType);
        }

        [Fact]
        public void Does_ulong_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(ulong));
            Assert.Equal("INT64", typeMapping.StoreType);
        }

        [Fact]
        public void Does_ushort_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(ushort));
            Assert.Equal("INT64", typeMapping.StoreType);
        }

        [Fact]
        public void Does_bytes_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(byte[]));
            // Migration Sql Generator will replace `BYTES` by `BYTES(MAX)`
            Assert.Equal("BYTES", typeMapping.StoreType);
        }

        [Fact]
        public void Does_decimal_array_mapping()
        {
            Assert.Equal("ARRAY<NUMERIC>", GetTypeMapping(typeof(decimal[])).StoreType);
        }

        [Fact]
        public void Does_nullable_decimal_array_mapping()
        {
            Assert.Equal("ARRAY<NUMERIC>", GetTypeMapping(typeof(decimal?[])).StoreType);
        }

        [Fact]
        public void Does_decimal_list_mapping()
        {
            Assert.Equal("ARRAY<NUMERIC>", GetTypeMapping(typeof(List<decimal>)).StoreType);
        }

        [Fact]
        public void Does_nullable_decimal_list_mapping()
        {
            Assert.Equal("ARRAY<NUMERIC>", GetTypeMapping(typeof(List<decimal?>)).StoreType);
        }

        [Fact]
        public void Does_spanner_numeric_array_mapping()
        {
            Assert.Equal("ARRAY<NUMERIC>", GetTypeMapping(typeof(SpannerNumeric[])).StoreType);
        }

        [Fact]
        public void Does_nullable_spanner_numeric_array_mapping()
        {
            Assert.Equal("ARRAY<NUMERIC>", GetTypeMapping(typeof(SpannerNumeric?[])).StoreType);
        }

        [Fact]
        public void Does_spanner_numeric_list_mapping()
        {
            Assert.Equal("ARRAY<NUMERIC>", GetTypeMapping(typeof(List<SpannerNumeric>)).StoreType);
        }

        [Fact]
        public void Does_nullable_spanner_numeric_list_mapping()
        {
            Assert.Equal("ARRAY<NUMERIC>", GetTypeMapping(typeof(List<SpannerNumeric?>)).StoreType);
        }

        [Fact]
        public void Does_string_array_mapping()
        {
            // Migration Sql Generator will replace `ARRAY<STRING>` by `ARRAY<STRING(MAX)>`
            Assert.Equal("ARRAY<STRING>", GetTypeMapping(typeof(string[])).StoreType);
        }

        [Fact]
        public void Does_string_list_mapping()
        {
            // Migration Sql Generator will replace `ARRAY<STRING>` by `ARRAY<STRING(MAX)>`
            Assert.Equal("ARRAY<STRING>", GetTypeMapping(typeof(List<string>)).StoreType);
        }

        [Fact]
        public void Does_bool_array_mapping()
        {
            Assert.Equal("ARRAY<BOOL>", GetTypeMapping(typeof(bool[])).StoreType);
        }

        [Fact]
        public void Does_nullable_bool_array_mapping()
        {
            Assert.Equal("ARRAY<BOOL>", GetTypeMapping(typeof(bool?[])).StoreType);
        }

        [Fact]
        public void Does_bool_list_mapping()
        {
            Assert.Equal("ARRAY<BOOL>", GetTypeMapping(typeof(List<bool>)).StoreType);
        }

        [Fact]
        public void Does_nullable_bool_list_mapping()
        {
            Assert.Equal("ARRAY<BOOL>", GetTypeMapping(typeof(List<bool?>)).StoreType);
        }

        [Fact]
        public void Does_double_array_mapping()
        {
            Assert.Equal("ARRAY<FLOAT64>", GetTypeMapping(typeof(double[])).StoreType);
        }

        [Fact]
        public void Does_nullable_double_array_mapping()
        {
            Assert.Equal("ARRAY<FLOAT64>", GetTypeMapping(typeof(double?[])).StoreType);
        }

        [Fact]
        public void Does_double_list_mapping()
        {
            Assert.Equal("ARRAY<FLOAT64>", GetTypeMapping(typeof(List<double>)).StoreType);
        }

        [Fact]
        public void Does_nullable_double_list_mapping()
        {
            Assert.Equal("ARRAY<FLOAT64>", GetTypeMapping(typeof(List<double?>)).StoreType);
        }

        [Fact]
        public void Does_long_array_mapping()
        {
            Assert.Equal("ARRAY<INT64>", GetTypeMapping(typeof(long[])).StoreType);
        }

        [Fact]
        public void Does_nullable_long_array_mapping()
        {
            Assert.Equal("ARRAY<INT64>", GetTypeMapping(typeof(long?[])).StoreType);
        }

        [Fact]
        public void Does_long_list_mapping()
        {
            Assert.Equal("ARRAY<INT64>", GetTypeMapping(typeof(List<long>)).StoreType);
        }

        [Fact]
        public void Does_nullable_long_list_mapping()
        {
            Assert.Equal("ARRAY<INT64>", GetTypeMapping(typeof(List<long?>)).StoreType);
        }

        [Fact]
        public void Does_spanner_date_array_mapping()
        {
            Assert.Equal("ARRAY<DATE>", GetTypeMapping(typeof(SpannerDate[])).StoreType);
        }

        [Fact]
        public void Does_nullable_spanner_date_array_mapping()
        {
            Assert.Equal("ARRAY<DATE>", GetTypeMapping(typeof(SpannerDate?[])).StoreType);
        }

        [Fact]
        public void Does_spanner_date_list_mapping()
        {
            Assert.Equal("ARRAY<DATE>", GetTypeMapping(typeof(List<SpannerDate>)).StoreType);
        }

        [Fact]
        public void Does_nullable_spanner_date_list_mapping()
        {
            Assert.Equal("ARRAY<DATE>", GetTypeMapping(typeof(List<SpannerDate?>)).StoreType);
        }

        [Fact]
        public void Does_spanner_timestamp_array_mapping()
        {
            Assert.Equal("ARRAY<TIMESTAMP>", GetTypeMapping(typeof(DateTime[])).StoreType);
        }

        [Fact]
        public void Does_nullable_spanner_timestamp_array_mapping()
        {
            Assert.Equal("ARRAY<TIMESTAMP>", GetTypeMapping(typeof(DateTime?[])).StoreType);
        }

        [Fact]
        public void Does_spanner_timestamp_list_mapping()
        {
            Assert.Equal("ARRAY<TIMESTAMP>", GetTypeMapping(typeof(List<DateTime>)).StoreType);
        }

        [Fact]
        public void Does_nullable_spanner_timestamp_list_mapping()
        {
            Assert.Equal("ARRAY<TIMESTAMP>", GetTypeMapping(typeof(List<DateTime?>)).StoreType);
        }

        [Fact]
        public void Does_bytes_array_mapping()
        {
            Assert.Equal("ARRAY<BYTES>", GetTypeMapping(typeof(byte[][])).StoreType);
        }

        [Fact]
        public void Does_bytes_list_mapping()
        {
            Assert.Equal("ARRAY<BYTES>", GetTypeMapping(typeof(List<byte[]>)).StoreType);
        }

        [Theory]
        [InlineData("BOOL", typeof(bool))]
        [InlineData("INT64", typeof(long))]
        [InlineData("FLOAT64", typeof(double))]
        [InlineData("NUMERIC", typeof(SpannerNumeric))]
        [InlineData("STRING(MAX)", typeof(string), null, true)]
        [InlineData("STRING(200)", typeof(string), 200, true)]
        [InlineData("BYTES(MAX)", typeof(byte[]))]
        [InlineData("BYTES(100)", typeof(byte[]), 100)]
        [InlineData("DATE", typeof(SpannerDate))]
        [InlineData("TIMESTAMP", typeof(DateTime))]
        public void Can_map_by_type_name(string typeName, System.Type clrType, int? size = null, bool unicode = false)
        {
            var mapping = CreateTypeMapper().FindMapping(typeName);

            Assert.Equal(clrType, mapping.ClrType);
            Assert.Equal(size, mapping.Size);
            Assert.Equal(unicode, mapping.IsUnicode);
            Assert.Equal(typeName, mapping.StoreType);
        }

        [Theory]
        [InlineData("ARRAY<BOOL>", typeof(List<bool?>))]
        [InlineData("ARRAY<INT64>", typeof(List<long?>))]
        [InlineData("ARRAY<FLOAT64>", typeof(List<double?>))]
        [InlineData("ARRAY<NUMERIC>", typeof(List<SpannerNumeric?>))]
        [InlineData("ARRAY<STRING(MAX)>", typeof(List<string>), null, true, "ARRAY<STRING>")]
        [InlineData("ARRAY<STRING(200)>", typeof(List<string>), 200, true, "ARRAY<STRING>")]
        [InlineData("ARRAY<BYTES(MAX)>", typeof(List<byte[]>), null, false, "ARRAY<BYTES>")]
        [InlineData("ARRAY<BYTES(100)>", typeof(List<byte[]>), 100, false, "ARRAY<BYTES>")]
        [InlineData("ARRAY<DATE>", typeof(List<SpannerDate?>))]
        [InlineData("ARRAY<TIMESTAMP>", typeof(List<DateTime?>))]
        public void Can_map_by_array_type_name(string typeName, System.Type clrType, int? size = null, bool unicode = false, string expectedType = null)
        {
            var mapping = CreateTypeMapper().FindMapping(typeName);

            Assert.Equal(clrType, mapping.ClrType);
            Assert.Equal(size, mapping.Size);
            Assert.Equal(unicode, mapping.IsUnicode);
            Assert.Equal(expectedType ?? typeName, mapping.StoreType);
        }

        private RelationalTypeMapping GetTypeMapping(
           System.Type propertyType,
           bool? nullable = null,
           int? maxLength = null,
           bool? unicode = null)
        {
            var property = CreateEntityType().AddProperty("MyProp", propertyType);

            if (nullable.HasValue)
            {
                property.IsNullable = nullable.Value;
            }

            if (maxLength.HasValue)
            {
                property.SetMaxLength(maxLength);
            }

            if (unicode.HasValue)
            {
                property.SetIsUnicode(unicode);
            }

            return CreateTypeMapper().GetMapping(property);
        }

        private static IRelationalTypeMappingSource CreateTypeMapper()
            => new SpannerTypeMappingSource(
                TestServiceFactory.Instance.Create<TypeMappingSourceDependencies>(),
                TestServiceFactory.Instance.Create<RelationalTypeMappingSourceDependencies>());
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
