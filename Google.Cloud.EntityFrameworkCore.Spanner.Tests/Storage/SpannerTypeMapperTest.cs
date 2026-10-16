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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.EntityFrameworkCore.Spanner.Tests.TestUtilities;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;
using SpannerDate = Google.Cloud.EntityFrameworkCore.Spanner.Storage.SpannerDate;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Storage
{
    using Type = System.Type;

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
            Assert.Equal("FLOAT32", GetTypeMapping(typeof(float)).StoreType);
        }

        [Theory]
        [InlineData("FLOAT32")]
        [InlineData("float32")]
        [InlineData("Float32")]
        public void Can_map_float32_by_name(string typeName)
        {
            var mapping = CreateTypeMapper().FindMapping(typeName);
            Assert.NotNull(mapping);
            Assert.Equal(typeof(float), mapping.ClrType);
            Assert.Equal(typeName, mapping.StoreType);
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
        public void Does_char_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(char));
            Assert.Equal("STRING(1)", typeMapping.StoreType);
        }

        [Fact]
        public void Does_guid_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(Guid));
            Assert.Equal("STRING(36)", typeMapping.StoreType);
            Assert.Equal(36, typeMapping.Size);
        }

        [Fact]
        public void Does_json_mapping()
        {
            Assert.Equal("JSON", GetTypeMapping(typeof(JsonDocument)).StoreType);
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
            var typeMapping = GetTypeMapping(typeof(decimal[]));
            Assert.Equal("ARRAY<NUMERIC>", typeMapping.StoreType);
            Assert.Equal(typeof(decimal[]), typeMapping.ClrType);
        }

        [Fact]
        public void Does_nullable_decimal_array_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(decimal?[]));
            Assert.Equal("ARRAY<NUMERIC>", typeMapping.StoreType);
            Assert.Equal(typeof(decimal?[]), typeMapping.ClrType);
        }

        [Fact]
        public void Does_decimal_list_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(List<decimal>));
            Assert.Equal("ARRAY<NUMERIC>", typeMapping.StoreType);
            Assert.Equal(typeof(List<decimal>), typeMapping.ClrType);
        }

        [Fact]
        public void Does_nullable_decimal_list_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(List<decimal?>));
            Assert.Equal("ARRAY<NUMERIC>", typeMapping.StoreType);
            Assert.Equal(typeof(List<decimal?>), typeMapping.ClrType);
        }

        [Fact]
        public void Does_spanner_numeric_array_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(SpannerNumeric[]));
            Assert.Equal("ARRAY<NUMERIC>", typeMapping.StoreType);
            Assert.Equal(typeof(SpannerNumeric[]), typeMapping.ClrType);
        }

        [Fact]
        public void Does_nullable_spanner_numeric_array_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(SpannerNumeric?[]));
            Assert.Equal("ARRAY<NUMERIC>", typeMapping.StoreType);
            Assert.Equal(typeof(SpannerNumeric?[]), typeMapping.ClrType);
        }

        [Fact]
        public void Does_spanner_numeric_list_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(List<SpannerNumeric>));
            Assert.Equal("ARRAY<NUMERIC>", typeMapping.StoreType);
            Assert.Equal(typeof(List<SpannerNumeric>), typeMapping.ClrType);
        }

        [Fact]
        public void Does_nullable_spanner_numeric_list_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(List<SpannerNumeric?>));
            Assert.Equal("ARRAY<NUMERIC>", typeMapping.StoreType);
            Assert.Equal(typeof(List<SpannerNumeric?>), typeMapping.ClrType);
        }

        [Fact]
        public void Does_regex_mapping()
        {
            var typeMapping = GetTypeMapping(typeof(Regex));
            Assert.Equal("STRING", typeMapping.StoreType);
            Assert.Equal(typeof(Regex), typeMapping.ClrType);
            Assert.NotNull(typeMapping.Converter);
            Assert.NotNull(typeMapping.Comparer);

            var regex = new Regex(@"^[a-z]+$", RegexOptions.IgnoreCase);
            var converted = typeMapping.Converter.ConvertToProvider(regex);
            Assert.Equal(@"^[a-z]+$", converted);
            var restored = (Regex)typeMapping.Converter.ConvertFromProvider(converted)!;
            Assert.Equal(@"^[a-z]+$", restored.ToString());

            Assert.True(typeMapping.Comparer.Equals(regex, new Regex(@"^[a-z]+$", RegexOptions.IgnoreCase)));
            Assert.False(typeMapping.Comparer.Equals(regex, new Regex(@"^[a-z]+$")));
            Assert.False(typeMapping.Comparer.Equals(regex, null));
            Assert.False(typeMapping.Comparer.Equals(null, regex));
            Assert.True(typeMapping.Comparer.Equals(null, null));

            var snapshot = (Regex)typeMapping.Comparer.Snapshot(regex)!;
            Assert.NotNull(snapshot);
            Assert.Equal(regex.ToString(), snapshot.ToString());
            Assert.Equal(regex.Options, snapshot.Options);
            Assert.Equal(typeMapping.Comparer.GetHashCode(regex), typeMapping.Comparer.GetHashCode(snapshot));
        }

        [Fact]
        public void Does_collection_interface_decimal_mapping()
        {
            var typeMapper = CreateTypeMapper();
            var enumerableMapping = typeMapper.FindMapping(typeof(IEnumerable<decimal>));
            Assert.NotNull(enumerableMapping);
            Assert.Equal("ARRAY<NUMERIC>", enumerableMapping.StoreType);
            Assert.Equal(typeof(decimal[]), enumerableMapping.ClrType);

            var hashSetMapping = typeMapper.FindMapping(typeof(HashSet<decimal>));
            Assert.NotNull(hashSetMapping);
            Assert.Equal("ARRAY<NUMERIC>", hashSetMapping.StoreType);
            Assert.Equal(typeof(decimal[]), hashSetMapping.ClrType);

            var nullableHashSetMapping = typeMapper.FindMapping(typeof(HashSet<decimal?>));
            Assert.NotNull(nullableHashSetMapping);
            Assert.Equal("ARRAY<NUMERIC>", nullableHashSetMapping.StoreType);
            Assert.Equal(typeof(decimal?[]), nullableHashSetMapping.ClrType);

            var spannerNumericHashSetMapping = typeMapper.FindMapping(typeof(HashSet<SpannerNumeric>));
            Assert.NotNull(spannerNumericHashSetMapping);
            Assert.Equal("ARRAY<NUMERIC>", spannerNumericHashSetMapping.StoreType);
            Assert.Equal(typeof(SpannerNumeric[]), spannerNumericHashSetMapping.ClrType);
        }

        [Fact]
        public void Can_build_model_with_decimals_and_regex()
        {
            var builder = CreateModelBuilder();
            builder.Entity<EntityWithDecimalsAndRegex>(entity =>
            {
                entity.HasKey(e => e.Id);
            });
            var model = builder.FinalizeModel();
            var entityType = model.FindEntityType(typeof(EntityWithDecimalsAndRegex));
            Assert.NotNull(entityType);

            var typeMapper = CreateTypeMapper();
            var decimalArrayProperty = entityType.FindProperty(nameof(EntityWithDecimalsAndRegex.DecimalArray));
            Assert.NotNull(decimalArrayProperty);
            var decimalArrayMapping = typeMapper.GetMapping(decimalArrayProperty);
            Assert.Equal("ARRAY<NUMERIC>", decimalArrayMapping.StoreType);
            Assert.Equal(typeof(decimal[]), decimalArrayMapping.ClrType);

            var nullableDecimalArrayProperty = entityType.FindProperty(nameof(EntityWithDecimalsAndRegex.NullableDecimalArray));
            Assert.NotNull(nullableDecimalArrayProperty);
            var nullableDecimalArrayMapping = typeMapper.GetMapping(nullableDecimalArrayProperty);
            Assert.Equal("ARRAY<NUMERIC>", nullableDecimalArrayMapping.StoreType);
            Assert.Equal(typeof(decimal?[]), nullableDecimalArrayMapping.ClrType);

            var decimalListProperty = entityType.FindProperty(nameof(EntityWithDecimalsAndRegex.DecimalList));
            Assert.NotNull(decimalListProperty);
            var decimalListMapping = typeMapper.GetMapping(decimalListProperty);
            Assert.Equal("ARRAY<NUMERIC>", decimalListMapping.StoreType);
            Assert.Equal(typeof(List<decimal>), decimalListMapping.ClrType);

            var nullableDecimalListProperty = entityType.FindProperty(nameof(EntityWithDecimalsAndRegex.NullableDecimalList));
            Assert.NotNull(nullableDecimalListProperty);
            var nullableDecimalListMapping = typeMapper.GetMapping(nullableDecimalListProperty);
            Assert.Equal("ARRAY<NUMERIC>", nullableDecimalListMapping.StoreType);
            Assert.Equal(typeof(List<decimal?>), nullableDecimalListMapping.ClrType);

            var regexProperty = entityType.FindProperty(nameof(EntityWithDecimalsAndRegex.Pattern));
            Assert.NotNull(regexProperty);
            var regexMapping = typeMapper.GetMapping(regexProperty);
            Assert.Equal("STRING", regexMapping.StoreType);
            Assert.Equal(typeof(Regex), regexMapping.ClrType);
            Assert.NotNull(regexMapping.Converter);
            Assert.NotNull(regexMapping.Comparer);
        }

        private class EntityWithDecimalsAndRegex
        {
            public long Id { get; set; }
            public decimal[] DecimalArray { get; set; }
            public decimal?[] NullableDecimalArray { get; set; }
            public List<decimal> DecimalList { get; set; }
            public List<decimal?> NullableDecimalList { get; set; }
            public Regex Pattern { get; set; }
        }

        [Fact]
        public void SpannerRegexTypeMapping_ConfiguresParameter()
        {
            var mapping = SpannerRegexTypeMapping.Default;
            var command = new SpannerCommand();
            var regex = new Regex(@"foo\bar'baz");
            var parameter = mapping.CreateParameter(command, "parameter", regex);

            Assert.IsType<SpannerParameter>(parameter);
            var spannerParameter = (SpannerParameter)parameter;
            Assert.Equal(SpannerDbType.String, spannerParameter.SpannerDbType);
            Assert.Equal(@"foo\bar'baz", spannerParameter.Value);
        }

        [Fact]
        public void SpannerRegexTypeMapping_GeneratesSqlLiteral()
        {
            var mapping = SpannerRegexTypeMapping.Default;
            var regex = new Regex("foo\\bar'baz\r\nqux");
            var literal = mapping.GenerateSqlLiteral(regex);
            Assert.Equal(@"'foo\\bar\'baz\r\nqux'", literal);
        }

        [Fact]
        public void DecimalArrayMapping_ConfiguresParameter()
        {
            var typeMapper = CreateTypeMapper();
            var mapping = typeMapper.FindMapping(typeof(decimal[]));
            Assert.NotNull(mapping);
            var command = new SpannerCommand();
            var values = new decimal[] { 1.2m, 3.4m };
            var parameter = mapping.CreateParameter(command, "parameter", values);

            Assert.IsType<SpannerParameter>(parameter);
            var spannerParameter = (SpannerParameter)parameter;
            Assert.Equal(SpannerDbType.ArrayOf(SpannerDbType.Numeric), spannerParameter.SpannerDbType);
            Assert.Equal(values, spannerParameter.Value);
        }

        [Fact]
        public void DecimalListMapping_ConfiguresParameter()
        {
            var typeMapper = CreateTypeMapper();
            var mapping = typeMapper.FindMapping(typeof(List<decimal>));
            Assert.NotNull(mapping);
            var command = new SpannerCommand();
            var values = new List<decimal> { 1.2m, 3.4m };
            var parameter = mapping.CreateParameter(command, "parameter", values);

            Assert.IsType<SpannerParameter>(parameter);
            var spannerParameter = (SpannerParameter)parameter;
            Assert.Equal(SpannerDbType.ArrayOf(SpannerDbType.Numeric), spannerParameter.SpannerDbType);
            Assert.Equal(values, spannerParameter.Value);
        }

        [Fact]
        public void NullableDecimalArrayMapping_ConfiguresParameter()
        {
            var typeMapper = CreateTypeMapper();
            var mapping = typeMapper.FindMapping(typeof(decimal?[]));
            Assert.NotNull(mapping);
            var command = new SpannerCommand();
            var values = new decimal?[] { 1.2m, null };
            var parameter = mapping.CreateParameter(command, "parameter", values);

            Assert.IsType<SpannerParameter>(parameter);
            var spannerParameter = (SpannerParameter)parameter;
            Assert.Equal(SpannerDbType.ArrayOf(SpannerDbType.Numeric), spannerParameter.SpannerDbType);
            Assert.Equal(values, spannerParameter.Value);
        }

        [Fact]
        public void NullableDecimalListMapping_ConfiguresParameter()
        {
            var typeMapper = CreateTypeMapper();
            var mapping = typeMapper.FindMapping(typeof(List<decimal?>));
            Assert.NotNull(mapping);
            var command = new SpannerCommand();
            var values = new List<decimal?> { 1.2m, null };
            var parameter = mapping.CreateParameter(command, "parameter", values);

            Assert.IsType<SpannerParameter>(parameter);
            var spannerParameter = (SpannerParameter)parameter;
            Assert.Equal(SpannerDbType.ArrayOf(SpannerDbType.Numeric), spannerParameter.SpannerDbType);
            Assert.Equal(values, spannerParameter.Value);
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
        public void Does_long_enumerable_property_mapping()
        {
            Assert.Equal("ARRAY<INT64>", GetTypeMapping(typeof(IEnumerable<long>)).StoreType);
        }

        [Theory]
        [InlineData(typeof(IEnumerable<long>), "ARRAY<INT64>")]
        [InlineData(typeof(IList<long>), "ARRAY<INT64>")]
        [InlineData(typeof(IReadOnlyList<long>), "ARRAY<INT64>")]
        [InlineData(typeof(ICollection<long>), "ARRAY<INT64>")]
        [InlineData(typeof(IReadOnlyCollection<long>), "ARRAY<INT64>")]
        [InlineData(typeof(ISet<long>), "ARRAY<INT64>")]
        [InlineData(typeof(IReadOnlySet<long>), "ARRAY<INT64>")]
        [InlineData(typeof(HashSet<long>), "ARRAY<INT64>")]
        [InlineData(typeof(IEnumerable<int>), "ARRAY<INT64>")]
        [InlineData(typeof(IEnumerable<string>), "ARRAY<STRING>")]
        [InlineData(typeof(IEnumerable<bool>), "ARRAY<BOOL>")]
        [InlineData(typeof(IEnumerable<double>), "ARRAY<FLOAT64>")]
        [InlineData(typeof(IEnumerable<float>), "ARRAY<FLOAT32>")]
        [InlineData(typeof(IEnumerable<decimal>), "ARRAY<NUMERIC>")]
        [InlineData(typeof(IEnumerable<SpannerNumeric>), "ARRAY<NUMERIC>")]
        [InlineData(typeof(IEnumerable<DateTime>), "ARRAY<TIMESTAMP>")]
        [InlineData(typeof(IEnumerable<SpannerDate>), "ARRAY<DATE>")]
        [InlineData(typeof(IEnumerable<JsonDocument>), "ARRAY<JSON>")]
        [InlineData(typeof(IEnumerable<byte[]>), "ARRAY<BYTES>")]
        [InlineData(typeof(IEnumerable<long?>), "ARRAY<INT64>")]
        [InlineData(typeof(IEnumerable<int?>), "ARRAY<INT64>")]
        [InlineData(typeof(IEnumerable<bool?>), "ARRAY<BOOL>")]
        [InlineData(typeof(IEnumerable<double?>), "ARRAY<FLOAT64>")]
        [InlineData(typeof(IEnumerable<float?>), "ARRAY<FLOAT32>")]
        [InlineData(typeof(IEnumerable<decimal?>), "ARRAY<NUMERIC>")]
        [InlineData(typeof(IEnumerable<SpannerNumeric?>), "ARRAY<NUMERIC>")]
        [InlineData(typeof(IEnumerable<DateTime?>), "ARRAY<TIMESTAMP>")]
        [InlineData(typeof(IEnumerable<SpannerDate?>), "ARRAY<DATE>")]
        [InlineData(typeof(IList<string>), "ARRAY<STRING>")]
        [InlineData(typeof(IReadOnlyList<DateTime>), "ARRAY<TIMESTAMP>")]
        [InlineData(typeof(ISet<int>), "ARRAY<INT64>")]
        [InlineData(typeof(HashSet<double>), "ARRAY<FLOAT64>")]
        [InlineData(typeof(ICollection<bool>), "ARRAY<BOOL>")]
        public void Can_map_collection_interfaces_and_types(Type collectionType, string expectedStoreType)
        {
            var mapping = CreateTypeMapper().FindMapping(collectionType);
            Assert.NotNull(mapping);
            Assert.Equal(expectedStoreType, mapping.StoreType);
        }

        [Fact]
        public void Does_not_throw_for_open_generic_types()
        {
            var mapper = CreateTypeMapper();
            Assert.Null(mapper.FindMapping(typeof(IEnumerable<>)));
            Assert.Null(mapper.FindMapping(typeof(List<>)));
            Assert.Null(mapper.FindMapping(typeof(IList<>)));
            Assert.Null(mapper.FindMapping(typeof(IReadOnlyList<>)));
            Assert.Null(mapper.FindMapping(typeof(ICollection<>)));
            Assert.Null(mapper.FindMapping(typeof(IReadOnlyCollection<>)));
            Assert.Null(mapper.FindMapping(typeof(ISet<>)));
            Assert.Null(mapper.FindMapping(typeof(IReadOnlySet<>)));
            Assert.Null(mapper.FindMapping(typeof(HashSet<>)));
        }

        [Fact]
        public void Does_not_map_unsupported_or_multidimensional_collection_to_spanner_arrays()
        {
            var mapper = CreateTypeMapper();
            Assert.Null(mapper.FindMapping(typeof(int[,])));
            Assert.Null(mapper.FindMapping(typeof(string[,,])));
            Assert.Null(mapper.FindMapping(typeof(IEnumerable<byte>)));
            Assert.Null(mapper.FindMapping(typeof(IEnumerable<Guid>)));

            // In EF Core 10, primitive collections without a native provider array mapping
            // (such as TimeSpan, which has a scalar converter to string) fall back to EF Core's
            // built-in JSON collection mapping (SpannerStringTypeMapping with store type STRING).
            var timeSpanMapping = mapper.FindMapping(typeof(IEnumerable<TimeSpan>));
            Assert.IsType<SpannerStringTypeMapping>(timeSpanMapping);
            Assert.Equal("STRING", timeSpanMapping.StoreType);
        }

        [Fact]
        public void Can_map_collection_with_store_type_facets()
        {
            var mapper = CreateTypeMapper();
            var enumerableMapping = mapper.FindMapping(typeof(IEnumerable<string>), "ARRAY<STRING(200)>");
            Assert.NotNull(enumerableMapping);
            Assert.Equal("ARRAY<STRING(200)>", enumerableMapping.StoreType);
            Assert.Equal(200, enumerableMapping.Size);

            var listMapping = mapper.FindMapping(typeof(IList<string>), "ARRAY<STRING(100)>");
            Assert.NotNull(listMapping);
            Assert.Equal("ARRAY<STRING(100)>", listMapping.StoreType);
            Assert.Equal(100, listMapping.Size);
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

        [Fact]
        public void Does_json_array_mapping()
        {
            Assert.Equal("ARRAY<JSON>", GetTypeMapping(typeof(JsonDocument[])).StoreType);
        }

        [Fact]
        public void Does_json_list_mapping()
        {
            Assert.Equal("ARRAY<JSON>", GetTypeMapping(typeof(List<JsonDocument>)).StoreType);
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
        [InlineData("JSON", typeof(JsonDocument))]
        public void Can_map_by_type_name(string typeName, Type clrType, int? size = null, bool unicode = false)
        {
            var mapping = CreateTypeMapper().FindMapping(typeName);

            Assert.NotNull(mapping);
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
        [InlineData("ARRAY<STRING(MAX)>", typeof(List<string>), null, true, "ARRAY<STRING(MAX)>")]
        [InlineData("ARRAY<STRING(200)>", typeof(List<string>), 200, true, "ARRAY<STRING(200)>")]
        [InlineData("ARRAY<BYTES(MAX)>", typeof(List<byte[]>), null, false, "ARRAY<BYTES(MAX)>")]
        [InlineData("ARRAY<BYTES(100)>", typeof(List<byte[]>), 100, false, "ARRAY<BYTES(100)>")]
        [InlineData("ARRAY<DATE>", typeof(List<SpannerDate?>))]
        [InlineData("ARRAY<TIMESTAMP>", typeof(List<DateTime?>))]
        [InlineData("ARRAY<JSON>", typeof(List<JsonDocument>))]
        public void Can_map_by_array_type_name(string typeName, Type clrType, int? size = null, bool unicode = false, string expectedType = null)
        {
            var mapping = CreateTypeMapper().FindMapping(typeName);

            Assert.NotNull(mapping);
            Assert.Equal(clrType, mapping.ClrType);
            Assert.Equal(size, mapping.Size);
            Assert.Equal(unicode, mapping.IsUnicode);
            Assert.Equal(expectedType ?? typeName, mapping.StoreType);
        }

        [Fact]
        public void Generates_correct_string_sql_literals()
        {
            var mapping = SpannerStringTypeMapping.Default;

            Assert.Equal("'''Marc'''", mapping.GenerateSqlLiteral("Marc"));
            Assert.Equal("''''''", mapping.GenerateSqlLiteral(string.Empty));
            Assert.Equal(@"'''Marc\'s'''", mapping.GenerateSqlLiteral("Marc's"));
            Assert.Equal(@"'''Marc\'s \'greatest\' hits'''", mapping.GenerateSqlLiteral("Marc's 'greatest' hits"));
            Assert.Equal(@"'''Marc\''''", mapping.GenerateSqlLiteral("Marc'"));
            Assert.Equal(@"'''Marc\'\''''", mapping.GenerateSqlLiteral("Marc''"));
            Assert.Equal(@"'''it\'\'\'s'''", mapping.GenerateSqlLiteral("it'''s"));
            Assert.Equal(@"'''C:\\Users\\Alice'''", mapping.GenerateSqlLiteral(@"C:\Users\Alice"));
            Assert.Equal(@"'''test\\'''", mapping.GenerateSqlLiteral(@"test\"));
            Assert.Equal(@"'''test\\\''''", mapping.GenerateSqlLiteral(@"test\'"));
            Assert.Equal(@"'''\'\'\' OR 1=1 --'''", mapping.GenerateSqlLiteral("''' OR 1=1 --"));
            Assert.Equal(@"'''\'\'\''''", mapping.GenerateSqlLiteral("'''"));
            Assert.Equal(@"'''\\\\'''", mapping.GenerateSqlLiteral(@"\\"));
            Assert.Equal(@"'''test\\\\\\\''''", mapping.GenerateSqlLiteral(@"test\\\'"));
            Assert.Equal("'''Line 1\nLine 2'''", mapping.GenerateSqlLiteral("Line 1\nLine 2"));
            Assert.Equal("'''Line 1\\r\nLine 2'''", mapping.GenerateSqlLiteral("Line 1\r\nLine 2"));
            Assert.Equal(@"'''test\u0000test'''", mapping.GenerateSqlLiteral("test\0test"));
            Assert.Equal("NULL", mapping.GenerateSqlLiteral(null));
        }

        [Fact]
        public void Generates_correct_char_sql_literals()
        {
            var mapping = SpannerCharTypeMapping.Default;

            Assert.Equal("'''a'''", mapping.GenerateSqlLiteral('a'));
            Assert.Equal(@"'''\''''", mapping.GenerateSqlLiteral('\''));
            Assert.Equal(@"'''\\'''", mapping.GenerateSqlLiteral('\\'));
            Assert.Equal(@"'''\u0000'''", mapping.GenerateSqlLiteral('\0'));
            Assert.Equal(@"'''\r'''", mapping.GenerateSqlLiteral('\r'));
            Assert.Equal("'''\n'''", mapping.GenerateSqlLiteral('\n'));
            Assert.Equal("'''\"'''", mapping.GenerateSqlLiteral('"'));
            Assert.Equal("NULL", mapping.GenerateSqlLiteral(null));

            var typeMapper = CreateTypeMapper();
            var charMapping = typeMapper.FindMapping(typeof(char));
            Assert.IsType<SpannerCharTypeMapping>(charMapping);
            Assert.Equal(@"'''\\'''", charMapping.GenerateSqlLiteral('\\'));
            Assert.Equal(@"'''\u0000'''", charMapping.GenerateSqlLiteral('\0'));

            var nullableCharMapping = typeMapper.FindMapping(typeof(char?));
            Assert.IsType<SpannerCharTypeMapping>(nullableCharMapping);
            Assert.Equal(@"'''a'''", nullableCharMapping.GenerateSqlLiteral((char?)'a'));
            Assert.Equal("NULL", nullableCharMapping.GenerateSqlLiteral((char?)null));
        }

        [Fact]
        public void Generates_correct_json_sql_literals_from_json_document()
        {
            var mapping = SpannerJsonTypeMapping.Default;

            using var simpleDocument = JsonDocument.Parse("""{"name":"Alice"}""");
            Assert.Equal("""JSON '{"name":"Alice"}'""", mapping.GenerateSqlLiteral(simpleDocument));

            using var quoteDocument = JsonDocument.Parse("""{"name":"Marc's"}""");
            Assert.Equal(@"JSON '{""name"":""Marc\'s""}'", mapping.GenerateSqlLiteral(quoteDocument));

            using var escapedQuoteDocument = JsonDocument.Parse("""{"quote":"He said \"Hi\""}""");
            Assert.Equal(@"JSON '{""quote"":""He said \\""Hi\\""""}'", mapping.GenerateSqlLiteral(escapedQuoteDocument));

            using var pathDocument = JsonDocument.Parse("""{"path":"C:\\Users"}""");
            Assert.Equal(@"JSON '{""path"":""C:\\\\Users""}'", mapping.GenerateSqlLiteral(pathDocument));

            using var scalarStringDocument = JsonDocument.Parse("\"Hello\"");
            Assert.Equal(@"JSON '""Hello""'", mapping.GenerateSqlLiteral(scalarStringDocument));
            Assert.Equal(@"JSON '""Hello""'", mapping.GenerateSqlLiteral(scalarStringDocument.RootElement));

            Assert.Equal("""JSON '{"name":"Alice"}'""", mapping.GenerateSqlLiteral(simpleDocument.RootElement));

            using var scalarNumberDocument = JsonDocument.Parse("42");
            Assert.Equal("JSON '42'", mapping.GenerateSqlLiteral(scalarNumberDocument.RootElement));
            Assert.Equal("NULL", mapping.GenerateSqlLiteral(null));
        }

        [Fact]
        public void Generates_correct_json_sql_literals_from_string()
        {
            var mapping = SpannerJsonTypeMapping.Default;

            Assert.Equal("""JSON '{"name":"Alice"}'""", mapping.GenerateSqlLiteral("""{"name":"Alice"}"""));
            Assert.Equal(@"JSON '{""name"":""Marc\'s""}'", mapping.GenerateSqlLiteral("""{"name":"Marc's"}"""));
            Assert.Equal(@"JSON '{""quote"":""He said \\""Hi\\""""}'", mapping.GenerateSqlLiteral("""{"quote":"He said \"Hi\""}"""));
            Assert.Equal(@"JSON '{""path"":""C:\\\\Users""}'", mapping.GenerateSqlLiteral("""{"path":"C:\\Users"}"""));
            Assert.Equal(@"JSON '{\n  ""name"": ""Alice""\n}'", mapping.GenerateSqlLiteral("{\n  \"name\": \"Alice\"\n}"));
            Assert.Equal(@"JSON '{\r\n  ""name"": ""Alice""\r\n}'", mapping.GenerateSqlLiteral("{\r\n  \"name\": \"Alice\"\r\n}"));
            Assert.Equal(@"JSON '{\n\t""name"": ""Alice""\n}'", mapping.GenerateSqlLiteral("{\n\t\"name\": \"Alice\"\n}"));
            Assert.Equal(@"JSON '{""key"":""\'; DROP TABLE Users; --""}'", mapping.GenerateSqlLiteral("""{"key":"'; DROP TABLE Users; --"}"""));
            Assert.Equal(@"JSON '{""name"":""Alice\\u0000Bob""}'", mapping.GenerateSqlLiteral("{\"name\":\"Alice\0Bob\"}"));
        }

        [Fact]
        public void Generates_correct_json_as_string_sql_literals()
        {
            var typeMapper = CreateTypeMapper();
            var mapping = typeMapper.FindMapping(typeof(string), "JSON");
            Assert.NotNull(mapping);
            Assert.IsType<SpannerJsonStringTypeMapping>(mapping);

            Assert.Equal("""JSON '{"name":"Alice"}'""", mapping.GenerateSqlLiteral("""{"name":"Alice"}"""));
            Assert.Equal(@"JSON '{""name"":""Marc\'s""}'", mapping.GenerateSqlLiteral("""{"name":"Marc's"}"""));
            Assert.Equal(@"JSON '{""path"":""C:\\\\Users""}'", mapping.GenerateSqlLiteral("""{"path":"C:\\Users"}"""));

            var lowercaseMapping = typeMapper.FindMapping(typeof(string), "json");
            Assert.NotNull(lowercaseMapping);
            Assert.IsType<SpannerJsonStringTypeMapping>(lowercaseMapping);
            Assert.Equal("""JSON '{"name":"Alice"}'""", lowercaseMapping.GenerateSqlLiteral("""{"name":"Alice"}"""));

            var defaultMapping = SpannerJsonStringTypeMapping.Default;
            Assert.Equal("""JSON '{"name":"Alice"}'""", defaultMapping.GenerateSqlLiteral("""{"name":"Alice"}"""));

            var cloned = mapping.Clone(new RelationalTypeMappingInfo(storeTypeName: "JSON"));
            Assert.IsType<SpannerJsonStringTypeMapping>(cloned);
            Assert.Equal("""JSON '{"name":"Alice"}'""", cloned.GenerateSqlLiteral("""{"name":"Alice"}"""));
        }

        [Fact]
        public void Generates_correct_structural_json_sql_literals()
        {
            var mapping = SpannerStructuralJsonTypeMapping.Default;

            Assert.Equal("""JSON '{"name":"Alice"}'""", mapping.GenerateSqlLiteral("""{"name":"Alice"}"""));
            Assert.Equal(@"JSON '{""title"":""Marc\'s Album""}'", mapping.GenerateSqlLiteral("""{"title":"Marc's Album"}"""));
            Assert.Equal(@"JSON '{""quote"":""\\""Total Junk\\""""}'", mapping.GenerateSqlLiteral("""{"quote":"\"Total Junk\""}"""));
            Assert.Equal(@"JSON '{""folder"":""C:\\\\Data""}'", mapping.GenerateSqlLiteral("""{"folder":"C:\\Data"}"""));
            Assert.Equal(@"JSON '{\n\t""name"": ""Alice""\n}'", mapping.GenerateSqlLiteral("{\n\t\"name\": \"Alice\"\n}"));
            Assert.Equal(@"JSON '{""key"":""\'; DROP TABLE Users; --""}'", mapping.GenerateSqlLiteral("""{"key":"'; DROP TABLE Users; --"}"""));

            using var jsonDocument = JsonDocument.Parse("""{"name":"Alice"}""");
            Assert.Equal("""JSON '{"name":"Alice"}'""", mapping.GenerateSqlLiteral(jsonDocument));
            Assert.Equal("""JSON '{"name":"Alice"}'""", mapping.GenerateSqlLiteral(jsonDocument.RootElement));

            using var scalarElement = JsonDocument.Parse("\"test\"");
            Assert.Equal(@"JSON '""test""'", mapping.GenerateSqlLiteral(scalarElement.RootElement));
            Assert.Equal("NULL", mapping.GenerateSqlLiteral(null));
        }

        [Fact]
        public void Generates_correct_json_array_sql_literals()
        {
            var mapping = SpannerJsonArrayTypeMapping.Default;

            Assert.Equal("NULL", mapping.GenerateSqlLiteral(null));
            Assert.Equal("ARRAY<JSON>[]", mapping.GenerateSqlLiteral(Array.Empty<JsonDocument>()));
            Assert.Equal("ARRAY<JSON>[]", mapping.GenerateSqlLiteral(Array.Empty<string>()));

            using var document1 = JsonDocument.Parse("""{"name":"Alice"}""");
            using var document2 = JsonDocument.Parse("""{"title":"Marc's Album"}""");
            using var document3 = JsonDocument.Parse("""{"path":"C:\\Users\\Alice"}""");

            var documents = new[] { document1, null, document2, document3 };
            Assert.Equal(
                @"ARRAY<JSON>[JSON '{""name"":""Alice""}', NULL, JSON '{""title"":""Marc\'s Album""}', JSON '{""path"":""C:\\\\Users\\\\Alice""}']",
                mapping.GenerateSqlLiteral(documents));

            var stringValues = new[] { """{"name":"Alice"}""", null, """{"title":"Marc's Album"}""" };
            Assert.Equal(
                @"ARRAY<JSON>[JSON '{""name"":""Alice""}', NULL, JSON '{""title"":""Marc\'s Album""}']",
                mapping.GenerateSqlLiteral(stringValues));

            using var scalarElement = JsonDocument.Parse("\"test\"");
            var elements = new[] { scalarElement.RootElement };
            Assert.Equal(
                @"ARRAY<JSON>[JSON '""test""']",
                mapping.GenerateSqlLiteral(elements));

            Assert.Throws<ArgumentException>(() => mapping.GenerateSqlLiteral("not an array"));
            Assert.Throws<ArgumentException>(() => mapping.GenerateSqlLiteral(42));
            Assert.Throws<ArgumentException>(() => mapping.GenerateSqlLiteral(new[] { 1, 2 }));
            Assert.Throws<ArgumentException>(() => mapping.GenerateSqlLiteral(Array.Empty<byte>()));
        }

        [Fact]
        public void Generates_correct_json_list_sql_literals()
        {
            var mapping = SpannerJsonListTypeMapping.Default;

            Assert.Equal("NULL", mapping.GenerateSqlLiteral(null));
            Assert.Equal("ARRAY<JSON>[]", mapping.GenerateSqlLiteral(new List<JsonDocument>()));
            Assert.Equal("ARRAY<JSON>[]", mapping.GenerateSqlLiteral(new List<string>()));

            using var document1 = JsonDocument.Parse("""{"name":"Alice"}""");
            using var document2 = JsonDocument.Parse("""{"title":"Marc's Album"}""");

            var documents = new List<JsonDocument> { document1, null, document2 };
            Assert.Equal(
                @"ARRAY<JSON>[JSON '{""name"":""Alice""}', NULL, JSON '{""title"":""Marc\'s Album""}']",
                mapping.GenerateSqlLiteral(documents));

            var stringValues = new List<string> { """{"name":"Alice"}""", null, """{"title":"Marc's Album"}""" };
            Assert.Equal(
                @"ARRAY<JSON>[JSON '{""name"":""Alice""}', NULL, JSON '{""title"":""Marc\'s Album""}']",
                mapping.GenerateSqlLiteral(stringValues));

            using var scalarElement = JsonDocument.Parse("\"test\"");
            var elements = new List<JsonElement> { scalarElement.RootElement };
            Assert.Equal(
                @"ARRAY<JSON>[JSON '""test""']",
                mapping.GenerateSqlLiteral(elements));

            Assert.Throws<ArgumentException>(() => mapping.GenerateSqlLiteral("not a list"));
            Assert.Throws<ArgumentException>(() => mapping.GenerateSqlLiteral(42));
            Assert.Throws<ArgumentException>(() => mapping.GenerateSqlLiteral(new List<int> { 1, 2 }));
            Assert.Throws<ArgumentException>(() => mapping.GenerateSqlLiteral(Array.Empty<byte>()));
        }

        [Fact]
        public void Default_type_mappings_have_initialized_converters()
        {
            Assert.Equal(typeof(SpannerDate), SpannerDateTypeMapping.Default.ClrType);
            Assert.Equal(typeof(DateOnly), SpannerDateOnlyTypeMapping.Default.ClrType);
            Assert.Equal(typeof(SpannerDate[]), SpannerDateArrayTypeMapping.Default.ClrType);
            Assert.Equal(typeof(List<SpannerDate>), SpannerDateListTypeMapping.Default.ClrType);
            Assert.Equal(typeof(SpannerDate?[]), SpannerNullableDateArrayTypeMapping.Default.ClrType);
            Assert.Equal(typeof(List<SpannerDate?>), SpannerNullableDateListTypeMapping.Default.ClrType);
            Assert.Equal(typeof(JsonDocument), SpannerJsonTypeMapping.Default.ClrType);
            Assert.Equal(typeof(JsonDocument[]), SpannerJsonArrayTypeMapping.Default.ClrType);
            Assert.Equal(typeof(List<JsonDocument>), SpannerJsonListTypeMapping.Default.ClrType);
            Assert.Equal(typeof(Regex), SpannerRegexTypeMapping.Default.ClrType);

            Assert.NotNull(SpannerDateTypeMapping.Default.Converter);
            Assert.NotNull(SpannerDateOnlyTypeMapping.Default.Converter);
            Assert.NotNull(SpannerDateArrayTypeMapping.Default.Converter);
            Assert.NotNull(SpannerDateListTypeMapping.Default.Converter);
            Assert.NotNull(SpannerNullableDateArrayTypeMapping.Default.Converter);
            Assert.NotNull(SpannerNullableDateListTypeMapping.Default.Converter);
            Assert.NotNull(SpannerJsonTypeMapping.Default.Converter);
            Assert.NotNull(SpannerJsonArrayTypeMapping.Default.Converter);
            Assert.NotNull(SpannerJsonListTypeMapping.Default.Converter);
            Assert.NotNull(SpannerRegexTypeMapping.Default.Converter);

            Assert.True(SpannerJsonTypeMapping.JsonOptions.AllowTrailingCommas);

            var dateConverter = SpannerDateTypeMapping.Default.Converter;
            var testDate = new SpannerDate(2025, 1, 15);
            var dateTime = (DateTime)dateConverter.ConvertToProvider(testDate)!;
            Assert.Equal(new DateTime(2025, 1, 15), dateTime);
            Assert.Equal(testDate, (SpannerDate)dateConverter.ConvertFromProvider(dateTime)!);

            var dateOnlyConverter = SpannerDateOnlyTypeMapping.Default.Converter;
            var testDateOnly = new DateOnly(2025, 1, 15);
            var dateOnlyDateTime = (DateTime)dateOnlyConverter.ConvertToProvider(testDateOnly)!;
            Assert.Equal(new DateTime(2025, 1, 15), dateOnlyDateTime);
            Assert.Equal(testDateOnly, (DateOnly)dateOnlyConverter.ConvertFromProvider(dateOnlyDateTime)!);

            var jsonConverter = SpannerJsonTypeMapping.Default.Converter;
            using var jsonDocument = JsonDocument.Parse("{\"key\":\"value\"}");
            var jsonString = (string)jsonConverter.ConvertToProvider(jsonDocument)!;
            Assert.Equal("{\"key\":\"value\"}", jsonString);
            using var parsedDocument = (JsonDocument)jsonConverter.ConvertFromProvider(jsonString)!;
            Assert.Equal("value", parsedDocument.RootElement.GetProperty("key").GetString());

            var dateArrayConverter = SpannerDateArrayTypeMapping.Default.Converter;
            var testDateArray = new[] { testDate };
            var dateArrayProvider = (DateTime[])dateArrayConverter.ConvertToProvider(testDateArray)!;
            Assert.Equal(new[] { new DateTime(2025, 1, 15) }, dateArrayProvider);
            var dateArrayRoundTrip = (SpannerDate[])dateArrayConverter.ConvertFromProvider(dateArrayProvider)!;
            Assert.Equal(testDateArray, dateArrayRoundTrip);

            var dateListConverter = SpannerDateListTypeMapping.Default.Converter;
            var testDateList = new List<SpannerDate> { testDate };
            var dateListProvider = (List<DateTime>)dateListConverter.ConvertToProvider(testDateList)!;
            Assert.Equal(new List<DateTime> { new DateTime(2025, 1, 15) }, dateListProvider);
            var dateListRoundTrip = (List<SpannerDate>)dateListConverter.ConvertFromProvider(dateListProvider)!;
            Assert.Equal(testDateList, dateListRoundTrip);

            var nullableDateArrayConverter = SpannerNullableDateArrayTypeMapping.Default.Converter;
            var testNullableDateArray = new SpannerDate?[] { testDate, null };
            var nullableDateArrayProvider = (DateTime?[])nullableDateArrayConverter.ConvertToProvider(testNullableDateArray)!;
            Assert.Equal(new DateTime?[] { new DateTime(2025, 1, 15), null }, nullableDateArrayProvider);
            var nullableDateArrayRoundTrip = (SpannerDate?[])nullableDateArrayConverter.ConvertFromProvider(nullableDateArrayProvider)!;
            Assert.Equal(testNullableDateArray, nullableDateArrayRoundTrip);

            var nullableDateListConverter = SpannerNullableDateListTypeMapping.Default.Converter;
            var testNullableDateList = new List<SpannerDate?> { testDate, null };
            var nullableDateListProvider = (List<DateTime?>)nullableDateListConverter.ConvertToProvider(testNullableDateList)!;
            Assert.Equal(new List<DateTime?> { new DateTime(2025, 1, 15), null }, nullableDateListProvider);
            var nullableDateListRoundTrip = (List<SpannerDate?>)nullableDateListConverter.ConvertFromProvider(nullableDateListProvider)!;
            Assert.Equal(testNullableDateList, nullableDateListRoundTrip);

            var jsonArrayConverter = SpannerJsonArrayTypeMapping.Default.Converter;
            using var jsonDocumentForArray = JsonDocument.Parse("{\"key\":\"value\"}");
            var testJsonArray = new[] { jsonDocumentForArray };
            var jsonArrayProvider = (string[])jsonArrayConverter.ConvertToProvider(testJsonArray)!;
            Assert.Equal(new[] { "{\"key\":\"value\"}" }, jsonArrayProvider);
            var jsonArrayRoundTrip = (JsonDocument[])jsonArrayConverter.ConvertFromProvider(jsonArrayProvider)!;
            Assert.Single(jsonArrayRoundTrip);
            Assert.Equal("value", jsonArrayRoundTrip[0].RootElement.GetProperty("key").GetString());

            using var scalarStringDoc = JsonDocument.Parse("\"hello\"");
            using var scalarNullDoc = JsonDocument.Parse("null");
            var testJsonArrayWithSpecial = new[] { jsonDocumentForArray, null, scalarStringDoc, scalarNullDoc };
            var jsonArrayProviderSpecial = (string[])jsonArrayConverter.ConvertToProvider(testJsonArrayWithSpecial)!;
            Assert.Equal(new[] { "{\"key\":\"value\"}", null, "\"hello\"", "null" }, jsonArrayProviderSpecial);
            var jsonArrayRoundTripSpecial = (JsonDocument[])jsonArrayConverter.ConvertFromProvider(jsonArrayProviderSpecial)!;
            Assert.Equal(4, jsonArrayRoundTripSpecial.Length);
            Assert.Equal("value", jsonArrayRoundTripSpecial[0].RootElement.GetProperty("key").GetString());
            Assert.Null(jsonArrayRoundTripSpecial[1]);
            Assert.Equal("hello", jsonArrayRoundTripSpecial[2].RootElement.GetString());
            Assert.Equal(JsonValueKind.Null, jsonArrayRoundTripSpecial[3].RootElement.ValueKind);
            Assert.Null(jsonArrayConverter.ConvertToProvider(null));
            Assert.Null(jsonArrayConverter.ConvertFromProvider(null));

            var jsonListConverter = SpannerJsonListTypeMapping.Default.Converter;
            Assert.Null(jsonListConverter.ConvertToProvider(null));
            Assert.Null(jsonListConverter.ConvertFromProvider(null));
            using var jsonDocumentForList = JsonDocument.Parse("{\"key\":\"value\"}");
            var testJsonList = new List<JsonDocument> { jsonDocumentForList };
            var jsonListProvider = (List<string>)jsonListConverter.ConvertToProvider(testJsonList)!;
            Assert.Equal(new List<string> { "{\"key\":\"value\"}" }, jsonListProvider);
            var jsonListRoundTrip = (List<JsonDocument>)jsonListConverter.ConvertFromProvider(jsonListProvider)!;
            Assert.Single(jsonListRoundTrip);
            Assert.Equal("value", jsonListRoundTrip[0].RootElement.GetProperty("key").GetString());

            var testJsonListWithSpecial = new List<JsonDocument> { jsonDocumentForList, null, scalarStringDoc, scalarNullDoc };
            var jsonListProviderSpecial = (List<string>)jsonListConverter.ConvertToProvider(testJsonListWithSpecial)!;
            Assert.Equal(new List<string> { "{\"key\":\"value\"}", null, "\"hello\"", "null" }, jsonListProviderSpecial);
            var jsonListRoundTripSpecial = (List<JsonDocument>)jsonListConverter.ConvertFromProvider(jsonListProviderSpecial)!;
            Assert.Equal(4, jsonListRoundTripSpecial.Count);
            Assert.Equal("value", jsonListRoundTripSpecial[0].RootElement.GetProperty("key").GetString());
            Assert.Null(jsonListRoundTripSpecial[1]);
            Assert.Equal("hello", jsonListRoundTripSpecial[2].RootElement.GetString());
            Assert.Equal(JsonValueKind.Null, jsonListRoundTripSpecial[3].RootElement.ValueKind);

            var regexConverter = SpannerRegexTypeMapping.Default.Converter;
            var testRegex = new Regex(@"^[a-z]+$", RegexOptions.IgnoreCase);
            var regexString = (string)regexConverter.ConvertToProvider(testRegex)!;
            Assert.Equal(@"^[a-z]+$", regexString);
            var regexRoundTrip = (Regex)regexConverter.ConvertFromProvider(regexString)!;
            Assert.Equal(@"^[a-z]+$", regexRoundTrip.ToString());
        }

        [Fact]
        public void Spanner_complex_type_mapping_initializes_array_types()
        {
            Assert.NotNull(SpannerComplexTypeMapping.Default);
            Assert.False(SpannerComplexTypeMapping.Default.IsArrayType);

            var int64ArrayMapping = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(long[]));
            Assert.True(int64ArrayMapping.IsArrayType);

            var float32ArrayMapping = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Float32), typeof(float[]));
            Assert.True(float32ArrayMapping.IsArrayType);

            var scalarMapping = new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(long));
            Assert.False(scalarMapping.IsArrayType);
        }

        private RelationalTypeMapping GetTypeMapping(
           Type propertyType,
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

            var model = property.DeclaringType.Model.FinalizeModel();
            return CreateTypeMapper().GetMapping(model.FindEntityType(property.DeclaringType.ClrType)!.GetProperty(property.Name));
        }

        private static IRelationalTypeMappingSource CreateTypeMapper()
            => new SpannerTypeMappingSource(
                TestServiceFactory.Instance.Create<TypeMappingSourceDependencies>(),
                TestServiceFactory.Instance.Create<RelationalTypeMappingSourceDependencies>());
    }
#pragma warning restore EF1001 // Internal EF Core API usage.
}
