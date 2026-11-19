// Copyright 2020, Google Inc. All rights reserved.
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

using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using IEnumerable = System.Collections.IEnumerable;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    using Type = System.Type;

    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerTypeMappingSource : RelationalTypeMappingSource
    {
        internal const int StringMax = 2621440;
        internal const int BytesMax = 10485760;

        // --- Non-array types ---

        private static readonly BoolTypeMapping s_bool
            = new SpannerBoolTypeMapping(SpannerDbType.Bool.ToString());

        private static readonly SpannerDateTypeMapping s_date = new SpannerDateTypeMapping();

        private static readonly SpannerDateOnlyTypeMapping s_dateOnly = new();

        private static readonly SpannerTimestampTypeMapping s_datetime = new SpannerTimestampTypeMapping();

        private static readonly StringTypeMapping s_defaultString
            = new SpannerStringTypeMapping(SpannerDbType.String.ToString(), unicode: true, sqlDbType: SpannerDbType.String);

        private static readonly SpannerCharTypeMapping s_char
            = SpannerCharTypeMapping.Default;

        private static readonly FloatTypeMapping s_float = new SpannerFloatTypeMapping();

        private static readonly DoubleTypeMapping s_double = new SpannerDoubleTypeMapping();

        private static readonly SpannerComplexTypeMapping s_int =
            new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(int));

        private static readonly LongTypeMapping s_long
            = new LongTypeMapping(SpannerDbType.Int64.ToString(), DbType.Int64);

        private static readonly SpannerComplexTypeMapping s_uint
            = new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(uint));

        private static readonly SpannerComplexTypeMapping s_short
            = new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(short));

        private static readonly SpannerDecimalTypeMapping s_decimal = new();

        private static readonly SpannerNumericTypeMapping s_numeric = new SpannerNumericTypeMapping();

        private static readonly SpannerJsonTypeMapping s_json = new SpannerJsonTypeMapping();
        private static readonly SpannerStructuralJsonTypeMapping s_structuralJson = new SpannerStructuralJsonTypeMapping("json");
        private static readonly SpannerJsonStringTypeMapping s_jsonAsString = SpannerJsonStringTypeMapping.Default;

        private static readonly SpannerGuidTypeMapping s_guid
            = new SpannerGuidTypeMapping("STRING(36)", DbType.String);

        private static readonly SpannerComplexTypeMapping s_byte
            = new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(byte));

        private static readonly SpannerComplexTypeMapping s_sbyte
            = new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(sbyte));

        private static readonly SpannerComplexTypeMapping s_ulong
            = new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(ulong));

        private static readonly SpannerComplexTypeMapping s_ushort
            = new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(ushort));

        private static readonly ByteArrayTypeMapping s_bytes
            = new ByteArrayTypeMapping(SpannerDbType.Bytes.ToString(), DbType.Binary);


        // --- Array Types ---

        private static readonly SpannerComplexTypeMapping s_byteArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bytes), typeof(byte[][]));

        private static readonly SpannerComplexTypeMapping s_byteList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bytes), typeof(List<byte[]>));

        private static readonly SpannerComplexTypeMapping s_stringArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(string[]), true);

        private static readonly SpannerComplexTypeMapping s_stringList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(List<string>), true);

        private static readonly SpannerComplexTypeMapping s_nullableBoolArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bool), typeof(bool?[]));
        private static readonly SpannerComplexTypeMapping s_boolArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bool), typeof(bool[]));

        private static readonly SpannerComplexTypeMapping s_nullableBoolList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bool), typeof(List<bool?>));
        private static readonly SpannerComplexTypeMapping s_boolList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bool), typeof(List<bool>));

        private static readonly SpannerComplexTypeMapping s_nullableFloatArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Float32), typeof(float?[]));
        private static readonly SpannerComplexTypeMapping s_floatArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Float32), typeof(float[]));

        private static readonly SpannerComplexTypeMapping s_nullableFloatList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Float32), typeof(List<float?>));
        private static readonly SpannerComplexTypeMapping s_floatList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Float32), typeof(List<float>));

        private static readonly SpannerComplexTypeMapping s_nullableDoubleArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Float64), typeof(double?[]));
        private static readonly SpannerComplexTypeMapping s_doubleArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Float64), typeof(double[]));

        private static readonly SpannerComplexTypeMapping s_nullableDoubleList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Float64), typeof(List<double?>));
        private static readonly SpannerComplexTypeMapping s_doubleList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Float64), typeof(List<double>));

        private static readonly SpannerComplexTypeMapping s_nullableLongArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(long?[]));
        private static readonly SpannerComplexTypeMapping s_longArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(long[]));

        private static readonly SpannerComplexTypeMapping s_nullableLongList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<long?>));
        private static readonly SpannerComplexTypeMapping s_longList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<long>));

        private static readonly SpannerComplexTypeMapping s_nullableIntArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(int?[]));
        private static readonly SpannerComplexTypeMapping s_intArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(int[]));

        private static readonly SpannerComplexTypeMapping s_nullableIntList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<int?>));
        private static readonly SpannerComplexTypeMapping s_intList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<int>));

        private static readonly SpannerNullableDateArrayTypeMapping s_nullableDateArray = new SpannerNullableDateArrayTypeMapping();
        private static readonly SpannerDateArrayTypeMapping s_dateArray = new SpannerDateArrayTypeMapping();

        private static readonly SpannerNullableDateListTypeMapping s_nullableDateList = new SpannerNullableDateListTypeMapping();
        private static readonly SpannerDateListTypeMapping s_dateList = new SpannerDateListTypeMapping();

        private static readonly SpannerComplexTypeMapping s_nullableTimestampArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Timestamp), typeof(DateTime?[]));
        private static readonly SpannerComplexTypeMapping s_timestampArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Timestamp), typeof(DateTime[]));

        private static readonly SpannerComplexTypeMapping s_nullableTimestampList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Timestamp), typeof(List<DateTime?>));
        private static readonly SpannerComplexTypeMapping s_timestampList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Timestamp), typeof(List<DateTime>));

        private static readonly SpannerComplexTypeMapping s_nullableNumericArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Numeric), typeof(SpannerNumeric?[]));
        private static readonly SpannerComplexTypeMapping s_numericArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Numeric), typeof(SpannerNumeric[]));

        private static readonly SpannerComplexTypeMapping s_nullableNumericList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Numeric), typeof(List<SpannerNumeric?>));
        private static readonly SpannerComplexTypeMapping s_numericList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Numeric), typeof(List<SpannerNumeric>));

        private static readonly SpannerComplexTypeMapping s_nullableDecimalArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Numeric), typeof(decimal?[]));
        private static readonly SpannerComplexTypeMapping s_decimalArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Numeric), typeof(decimal[]));

        private static readonly SpannerComplexTypeMapping s_nullableDecimalList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Numeric), typeof(List<decimal?>));
        private static readonly SpannerComplexTypeMapping s_decimalList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Numeric), typeof(List<decimal>));

        private static readonly SpannerRegexTypeMapping s_regex = new();

        private static readonly SpannerJsonArrayTypeMapping s_jsonArray = new SpannerJsonArrayTypeMapping();
        private static readonly SpannerJsonListTypeMapping s_jsonList = new SpannerJsonListTypeMapping();

        private static readonly SpannerComplexTypeMapping s_jsonEnumerable = CreateEnumerableMapping<JsonDocument, string>(
            SpannerDbType.ArrayOf(SpannerDbType.Json),
            document => document == null ? null : document.RootElement.GetRawText(),
            json => json == null ? null : JsonDocument.Parse(json, SpannerJsonTypeMapping.JsonOptions));

        private static readonly SpannerComplexTypeMapping s_dateEnumerable = CreateEnumerableMapping<SpannerDate, DateTime>(
            SpannerDbType.ArrayOf(SpannerDbType.Date),
            date => date.ToDateTime(),
            dateTime => SpannerDate.FromDateTime(dateTime));
        private static readonly SpannerComplexTypeMapping s_nullableDateEnumerable = CreateEnumerableMapping<SpannerDate?, DateTime?>(
            SpannerDbType.ArrayOf(SpannerDbType.Date),
            date => date.HasValue ? date.Value.ToDateTime() : null,
            dateTime => dateTime.HasValue ? SpannerDate.FromDateTime(dateTime.Value) : null);

        private static readonly Func<DateOnly, DateTime> s_dateOnlyToDateTime = date => date.ToDateTime(TimeOnly.MinValue);
        private static readonly Func<DateTime, DateOnly> s_dateTimeToDateOnly = dateTime => DateOnly.FromDateTime(dateTime);
        private static readonly Func<DateOnly?, DateTime?> s_nullableDateOnlyToDateTime = date => date.HasValue ? date.Value.ToDateTime(TimeOnly.MinValue) : null;
        private static readonly Func<DateTime?, DateOnly?> s_nullableDateTimeToDateOnly = dateTime => dateTime.HasValue ? DateOnly.FromDateTime(dateTime.Value) : null;

        private static readonly SpannerComplexTypeMapping s_dateOnlyArray
            = CreateArrayMapping(SpannerDbType.ArrayOf(SpannerDbType.Date), s_dateOnlyToDateTime, s_dateTimeToDateOnly);
        private static readonly SpannerComplexTypeMapping s_nullableDateOnlyArray
            = CreateArrayMapping(SpannerDbType.ArrayOf(SpannerDbType.Date), s_nullableDateOnlyToDateTime, s_nullableDateTimeToDateOnly);
        private static readonly SpannerComplexTypeMapping s_dateOnlyList
            = CreateListMapping(SpannerDbType.ArrayOf(SpannerDbType.Date), s_dateOnlyToDateTime, s_dateTimeToDateOnly);
        private static readonly SpannerComplexTypeMapping s_nullableDateOnlyList
            = CreateListMapping(SpannerDbType.ArrayOf(SpannerDbType.Date), s_nullableDateOnlyToDateTime, s_nullableDateTimeToDateOnly);
        private static readonly SpannerComplexTypeMapping s_dateOnlyEnumerable
            = CreateEnumerableMapping(SpannerDbType.ArrayOf(SpannerDbType.Date), s_dateOnlyToDateTime, s_dateTimeToDateOnly);
        private static readonly SpannerComplexTypeMapping s_nullableDateOnlyEnumerable
            = CreateEnumerableMapping(SpannerDbType.ArrayOf(SpannerDbType.Date), s_nullableDateOnlyToDateTime, s_nullableDateTimeToDateOnly);

        private static readonly SpannerComplexTypeMapping s_guidArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(Guid[]), true);
        private static readonly SpannerComplexTypeMapping s_nullableGuidArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(Guid?[]), true);
        private static readonly SpannerComplexTypeMapping s_guidList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(List<Guid>), true);
        private static readonly SpannerComplexTypeMapping s_nullableGuidList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(List<Guid?>), true);

        private static readonly SpannerComplexTypeMapping s_shortArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(short[]));
        private static readonly SpannerComplexTypeMapping s_nullableShortArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(short?[]));
        private static readonly SpannerComplexTypeMapping s_shortList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<short>));
        private static readonly SpannerComplexTypeMapping s_nullableShortList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<short?>));

        private static readonly SpannerComplexTypeMapping s_byteListInt
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<byte>));
        private static readonly SpannerComplexTypeMapping s_nullableByteListInt
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<byte?>));
        private static readonly SpannerComplexTypeMapping s_nullableByteArrayInt
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(byte?[]));

        private static readonly SpannerComplexTypeMapping s_sbyteArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(sbyte[]));
        private static readonly SpannerComplexTypeMapping s_nullableSbyteArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(sbyte?[]));
        private static readonly SpannerComplexTypeMapping s_sbyteList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<sbyte>));
        private static readonly SpannerComplexTypeMapping s_nullableSbyteList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<sbyte?>));

        private static readonly SpannerComplexTypeMapping s_uintArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(uint[]));
        private static readonly SpannerComplexTypeMapping s_nullableUintArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(uint?[]));
        private static readonly SpannerComplexTypeMapping s_uintList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<uint>));
        private static readonly SpannerComplexTypeMapping s_nullableUintList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<uint?>));

        private static readonly SpannerComplexTypeMapping s_ushortArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(ushort[]));
        private static readonly SpannerComplexTypeMapping s_nullableUshortArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(ushort?[]));
        private static readonly SpannerComplexTypeMapping s_ushortList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<ushort>));
        private static readonly SpannerComplexTypeMapping s_nullableUshortList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<ushort?>));

        private static readonly SpannerComplexTypeMapping s_ulongArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(ulong[]));
        private static readonly SpannerComplexTypeMapping s_nullableUlongArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(ulong?[]));
        private static readonly SpannerComplexTypeMapping s_ulongList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<ulong>));
        private static readonly SpannerComplexTypeMapping s_nullableUlongList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), typeof(List<ulong?>));

        private static readonly SpannerComplexTypeMapping s_charArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(char[]), true);
        private static readonly SpannerComplexTypeMapping s_nullableCharArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(char?[]), true);
        private static readonly SpannerComplexTypeMapping s_charList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(List<char>), true);
        private static readonly SpannerComplexTypeMapping s_nullableCharList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(List<char?>), true);

        private static readonly ConcurrentDictionary<Type, RelationalTypeMapping> s_enumArrayMappings = new();

        private readonly Dictionary<Type, RelationalTypeMapping> _clrTypeMappings;

        private readonly Dictionary<string, RelationalTypeMapping> _storeTypeMappings;

        private readonly Dictionary<Type, RelationalTypeMapping> _elementClrTypeMappings;

        public SpannerTypeMappingSource(
            TypeMappingSourceDependencies dependencies,
            RelationalTypeMappingSourceDependencies relationalDependencies)
            : base(dependencies, relationalDependencies)
        {
            _clrTypeMappings
                = new Dictionary<Type, RelationalTypeMapping>
                {
                    {typeof(short), s_short},
                    {typeof(int), s_int},
                    {typeof(long), s_long},
                    {typeof(decimal), s_decimal},
                    {typeof(SpannerNumeric), s_numeric},
                    {typeof(JsonDocument), s_json},
                    {typeof(JsonElement), s_structuralJson},
                    {typeof(JsonTypePlaceholder), s_structuralJson},
                    {typeof(uint), s_uint},
                    {typeof(bool), s_bool},
                    {typeof(SpannerDate), s_date},
                    {typeof(DateOnly), s_dateOnly},
                    {typeof(DateTime), s_datetime},
                    {typeof(float), s_float},
                    {typeof(double), s_double},
                    {typeof(string), s_defaultString},
                    {typeof(char), s_char},
                    {typeof(Guid), s_guid},
                    {typeof(Regex), s_regex},
                    {typeof(byte), s_byte},
                    {typeof(sbyte), s_sbyte},
                    {typeof(ulong), s_ulong},
                    {typeof(ushort), s_ushort},
                    {typeof(byte[]), s_bytes},

                    {typeof(decimal[]), s_decimalArray},
                    {typeof(decimal?[]), s_nullableDecimalArray},
                    {typeof(SpannerNumeric[]), s_numericArray},
                    {typeof(SpannerNumeric?[]), s_nullableNumericArray},
                    {typeof(List<decimal>), s_decimalList},
                    {typeof(List<decimal?>), s_nullableDecimalList},
                    {typeof(List<SpannerNumeric>), s_numericList},
                    {typeof(List<SpannerNumeric?>), s_nullableNumericList},
                    {typeof(JsonDocument[]), s_jsonArray},
                    {typeof(List<JsonDocument>), s_jsonList},
                    {typeof(string[]), s_stringArray},
                    {typeof(List<string>), s_stringList},
                    {typeof(bool[]), s_boolArray},
                    {typeof(bool?[]), s_nullableBoolArray},
                    {typeof(List<bool>), s_boolList},
                    {typeof(List<bool?>), s_nullableBoolList},
                    {typeof(float[]), s_floatArray},
                    {typeof(float?[]), s_nullableFloatArray},
                    {typeof(List<float>), s_floatList},
                    {typeof(List<float?>), s_nullableFloatList},
                    {typeof(double[]), s_doubleArray},
                    {typeof(double?[]), s_nullableDoubleArray},
                    {typeof(List<double>), s_doubleList},
                    {typeof(List<double?>), s_nullableDoubleList},
                    {typeof(int[]), s_intArray},
                    {typeof(int?[]), s_nullableIntArray},
                    {typeof(List<int>), s_intList},
                    {typeof(List<int?>), s_nullableIntList},
                    {typeof(long[]), s_longArray},
                    {typeof(long?[]), s_nullableLongArray},
                    {typeof(List<long>), s_longList},
                    {typeof(List<long?>), s_nullableLongList},
                    {typeof(SpannerDate[]), s_dateArray},
                    {typeof(SpannerDate?[]), s_nullableDateArray},
                    {typeof(List<SpannerDate>), s_dateList},
                    {typeof(List<SpannerDate?>), s_nullableDateList},
                    {typeof(List<DateTime>), s_timestampList},
                    {typeof(List<DateTime?>), s_nullableTimestampList},
                    {typeof(DateTime[]), s_timestampArray},
                    {typeof(DateTime?[]), s_nullableTimestampArray},
                    {typeof(DateOnly[]), s_dateOnlyArray},
                    {typeof(DateOnly?[]), s_nullableDateOnlyArray},
                    {typeof(List<DateOnly>), s_dateOnlyList},
                    {typeof(List<DateOnly?>), s_nullableDateOnlyList},
                    {typeof(Guid[]), s_guidArray},
                    {typeof(Guid?[]), s_nullableGuidArray},
                    {typeof(List<Guid>), s_guidList},
                    {typeof(List<Guid?>), s_nullableGuidList},
                    {typeof(short[]), s_shortArray},
                    {typeof(short?[]), s_nullableShortArray},
                    {typeof(List<short>), s_shortList},
                    {typeof(List<short?>), s_nullableShortList},
                    {typeof(byte?[]), s_nullableByteArrayInt},
                    {typeof(List<byte>), s_byteListInt},
                    {typeof(List<byte?>), s_nullableByteListInt},
                    {typeof(sbyte[]), s_sbyteArray},
                    {typeof(sbyte?[]), s_nullableSbyteArray},
                    {typeof(List<sbyte>), s_sbyteList},
                    {typeof(List<sbyte?>), s_nullableSbyteList},
                    {typeof(uint[]), s_uintArray},
                    {typeof(uint?[]), s_nullableUintArray},
                    {typeof(List<uint>), s_uintList},
                    {typeof(List<uint?>), s_nullableUintList},
                    {typeof(ushort[]), s_ushortArray},
                    {typeof(ushort?[]), s_nullableUshortArray},
                    {typeof(List<ushort>), s_ushortList},
                    {typeof(List<ushort?>), s_nullableUshortList},
                    {typeof(ulong[]), s_ulongArray},
                    {typeof(ulong?[]), s_nullableUlongArray},
                    {typeof(List<ulong>), s_ulongList},
                    {typeof(List<ulong?>), s_nullableUlongList},
                    {typeof(char[]), s_charArray},
                    {typeof(char?[]), s_nullableCharArray},
                    {typeof(List<char>), s_charList},
                    {typeof(List<char?>), s_nullableCharList},
                    {typeof(byte[][]), s_byteArray},
                    {typeof(List<byte[]>), s_byteList},
                };

            _storeTypeMappings = new Dictionary<string, RelationalTypeMapping>(StringComparer.OrdinalIgnoreCase)
            {
                {SpannerDbType.Bool.ToString(), s_bool},
                {SpannerDbType.Bytes.ToString(), s_bytes},
                {SpannerDbType.Date.ToString(), s_date},
                {SpannerDbType.Float32.ToString(), s_float},
                {SpannerDbType.Float64.ToString(), s_double},
                {SpannerDbType.Int64.ToString(), s_long},
                {SpannerDbType.Timestamp.ToString(), s_datetime},
                {SpannerDbType.String.ToString(), s_defaultString},
                {SpannerDbType.Numeric.ToString(), s_numeric},
                {SpannerDbType.Json.ToString(), s_json},
                {"ARRAY<BOOL>", s_nullableBoolList},
                {"ARRAY<BYTES>", s_byteList},
                {"ARRAY<DATE>", s_nullableDateList},
                {"ARRAY<FLOAT32>", s_nullableFloatList},
                {"ARRAY<FLOAT64>", s_nullableDoubleList},
                {"ARRAY<INT64>", s_nullableLongList},
                {"ARRAY<STRING>", s_stringList},
                {"ARRAY<TIMESTAMP>", s_nullableTimestampList},
                {"ARRAY<NUMERIC>", s_nullableNumericList},
                {"ARRAY<JSON>", s_jsonList}
            };

            _elementClrTypeMappings = new Dictionary<Type, RelationalTypeMapping>
            {
                {typeof(int), s_intArray},
                {typeof(int?), s_nullableIntArray},
                {typeof(long), s_longArray},
                {typeof(long?), s_nullableLongArray},
                {typeof(short), s_shortArray},
                {typeof(short?), s_nullableShortArray},
                {typeof(byte), s_byteListInt},
                {typeof(byte?), s_nullableByteArrayInt},
                {typeof(sbyte), s_sbyteArray},
                {typeof(sbyte?), s_nullableSbyteArray},
                {typeof(uint), s_uintArray},
                {typeof(uint?), s_nullableUintArray},
                {typeof(ushort), s_ushortArray},
                {typeof(ushort?), s_nullableUshortArray},
                {typeof(ulong), s_ulongArray},
                {typeof(ulong?), s_nullableUlongArray},
                {typeof(decimal), s_decimalArray},
                {typeof(decimal?), s_nullableDecimalArray},
                {typeof(SpannerNumeric), s_numericArray},
                {typeof(SpannerNumeric?), s_nullableNumericArray},
                {typeof(string), s_stringArray},
                {typeof(char), s_charArray},
                {typeof(char?), s_nullableCharArray},
                {typeof(Guid), s_guidArray},
                {typeof(Guid?), s_nullableGuidArray},
                {typeof(bool), s_boolArray},
                {typeof(bool?), s_nullableBoolArray},
                {typeof(float), s_floatArray},
                {typeof(float?), s_nullableFloatArray},
                {typeof(double), s_doubleArray},
                {typeof(double?), s_nullableDoubleArray},
                {typeof(DateTime), s_timestampArray},
                {typeof(DateTime?), s_nullableTimestampArray},
                {typeof(DateOnly), s_dateOnlyEnumerable},
                {typeof(DateOnly?), s_nullableDateOnlyEnumerable},
                {typeof(SpannerDate), s_dateEnumerable},
                {typeof(SpannerDate?), s_nullableDateEnumerable},
                {typeof(JsonDocument), s_jsonEnumerable},
                {typeof(byte[]), s_byteArray},
            };
        }

        protected override string ParseStoreTypeName(
            string storeTypeName,
            ref bool? unicode,
            ref int? size,
            ref int? precision,
            ref int? scale)
        {
            var parsedName = base.ParseStoreTypeName(storeTypeName, ref unicode, ref size, ref precision, ref scale);
            return NormalizeStoreTypeNameBase(storeTypeName, parsedName);
        }

        protected override RelationalTypeMapping FindMapping(in RelationalTypeMappingInfo mappingInfo)
            => FindRawMapping(mappingInfo)?.Clone(mappingInfo)
                ?? base.FindMapping(mappingInfo);

#pragma warning disable EF1001
        protected override RelationalTypeMapping FindCollectionMapping(
            RelationalTypeMappingInfo info,
            Type modelType,
            Type providerType,
            CoreTypeMapping elementMapping)
        {
            if (info.StoreTypeName == null)
            {
                if (modelType != info.ClrType && FindByClrType(modelType) != null)
                {
                    return null;
                }
            }
            else if (FindByStoreType(info.StoreTypeName, info.StoreTypeNameBase, typeof(string)) == null
                     || FindByClrType(modelType) != null)
            {
                return null;
            }

            return base.FindCollectionMapping(info, modelType, providerType, elementMapping);
        }
#pragma warning restore EF1001

        private RelationalTypeMapping FindRawMapping(RelationalTypeMappingInfo mappingInfo)
        {
            var clrType = mappingInfo.ClrType;
            var storeTypeName = mappingInfo.StoreTypeName;
            var storeTypeNameBase = mappingInfo.StoreTypeNameBase;

            if (storeTypeName != null)
            {
                return FindByStoreType(storeTypeName, storeTypeNameBase, clrType);
            }

            if (clrType != null)
            {
                return FindByClrType(clrType);
            }

            return null;
        }

        private RelationalTypeMapping FindByStoreType(string storeTypeName, string storeTypeNameBase, Type clrType)
        {
            var normalizedBase = NormalizeStoreTypeNameBase(storeTypeName, storeTypeNameBase);

            if (storeTypeName.Equals("JSON", StringComparison.OrdinalIgnoreCase) &&
                clrType == typeof(string))
            {
                return s_jsonAsString;
            }

            if (_storeTypeMappings.TryGetValue(storeTypeName, out var mapping)
                || (normalizedBase != null && _storeTypeMappings.TryGetValue(normalizedBase, out mapping)))
            {
                if (clrType == null || mapping.ClrType == clrType)
                {
                    return mapping;
                }
            }

            if (clrType != null)
            {
                var clrMapping = FindByClrType(clrType);
                if (clrMapping != null && MatchesStoreType(clrMapping, storeTypeName, normalizedBase))
                {
                    return clrMapping;
                }
            }

            return null;
        }

        private RelationalTypeMapping FindByClrType(Type clrType)
        {
            if (_clrTypeMappings.TryGetValue(clrType, out var mapping))
            {
                return mapping;
            }

            if (TryGetElementType(clrType, out var elementType))
            {
                if (_elementClrTypeMappings.TryGetValue(elementType, out mapping))
                {
                    return mapping;
                }

                var unwrappedElementType = Nullable.GetUnderlyingType(elementType) ?? elementType;
                if (unwrappedElementType.IsEnum)
                {
                    return GetEnumArrayMapping(clrType, elementType, unwrappedElementType);
                }
            }

            return null;
        }

        private static string NormalizeStoreTypeNameBase(string storeTypeName, string storeTypeNameBase)
        {
            if ((string.Equals(storeTypeNameBase, "ARRAY<STRING", StringComparison.OrdinalIgnoreCase)
                 || string.Equals(storeTypeNameBase, "ARRAY<BYTES", StringComparison.OrdinalIgnoreCase))
                && storeTypeName != null
                && storeTypeName.TrimEnd().EndsWith(")>", StringComparison.Ordinal))
            {
                return storeTypeNameBase + ">";
            }

            return storeTypeNameBase;
        }

        private static bool MatchesStoreType(RelationalTypeMapping mapping, string storeTypeName, string normalizedBase)
        {
            if (string.Equals(mapping.StoreType, storeTypeName, StringComparison.OrdinalIgnoreCase)
                || (normalizedBase != null && string.Equals(mapping.StoreType, normalizedBase, StringComparison.OrdinalIgnoreCase)))
            {
                return true;
            }

            var mappingNormalizedBase = NormalizeStoreTypeNameBase(mapping.StoreType, mapping.StoreTypeNameBase);
            return mappingNormalizedBase != null
                && string.Equals(mappingNormalizedBase, normalizedBase, StringComparison.OrdinalIgnoreCase);
        }

        private static SpannerComplexTypeMapping CreateArrayMapping<TModelElement, TProviderElement>(
            SpannerDbType complexType,
            Func<TModelElement, TProviderElement> toProvider,
            Func<TProviderElement, TModelElement> fromProvider)
            => new(
                complexType,
                typeof(TModelElement[]),
                new ValueConverter<TModelElement[], TProviderElement[]>(
                    elements => elements.Select(toProvider).ToArray(),
                    elements => elements.Select(fromProvider).ToArray()));

        private static SpannerComplexTypeMapping CreateListMapping<TModelElement, TProviderElement>(
            SpannerDbType complexType,
            Func<TModelElement, TProviderElement> toProvider,
            Func<TProviderElement, TModelElement> fromProvider)
            => new(
                complexType,
                typeof(List<TModelElement>),
                new ValueConverter<List<TModelElement>, List<TProviderElement>>(
                    elements => elements.Select(toProvider).ToList(),
                    elements => elements.Select(fromProvider).ToList()));

        private static SpannerComplexTypeMapping CreateEnumerableMapping<TModelElement, TProviderElement>(
            SpannerDbType complexType,
            Func<TModelElement, TProviderElement> toProvider,
            Func<TProviderElement, TModelElement> fromProvider)
            => new(
                complexType,
                typeof(IEnumerable<TModelElement>),
                new ValueConverter<IEnumerable<TModelElement>, TProviderElement[]>(
                    elements => elements.Select(toProvider).ToArray(),
                    elements => elements.Select(fromProvider).ToArray()));

        private static RelationalTypeMapping GetEnumArrayMapping(Type collectionType, Type elementType, Type enumType)
            => s_enumArrayMappings.GetOrAdd(collectionType, _ =>
            {
                var isNullable = elementType != enumType;
                string factoryMethodName;
                if (collectionType.IsArray)
                {
                    factoryMethodName = isNullable ? nameof(CreateNullableEnumArrayMapping) : nameof(CreateEnumArrayMapping);
                }
                else if (collectionType.IsGenericType && collectionType.GetGenericTypeDefinition() == typeof(List<>))
                {
                    factoryMethodName = isNullable ? nameof(CreateNullableEnumListMapping) : nameof(CreateEnumListMapping);
                }
                else if (collectionType.IsGenericType && collectionType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    factoryMethodName = isNullable ? nameof(CreateNullableEnumEnumerableMapping) : nameof(CreateEnumEnumerableMapping);
                }
                else
                {
                    return GetEnumArrayMapping(typeof(IEnumerable<>).MakeGenericType(elementType), elementType, enumType);
                }

                var method = typeof(SpannerTypeMappingSource)
                    .GetMethod(factoryMethodName, BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(enumType);
                return (RelationalTypeMapping)method.Invoke(null, null)!;
            });

        private static class EnumConverter<TEnum> where TEnum : struct, Enum
        {
            internal static readonly Func<TEnum, long> ToLong = CompileToLong();
            internal static readonly Func<long, TEnum> FromLong = CompileFromLong();
            internal static readonly Func<TEnum?, long?> NullableToLong = enumValue => enumValue.HasValue ? ToLong(enumValue.Value) : null;
            internal static readonly Func<long?, TEnum?> NullableFromLong = longValue => longValue.HasValue ? FromLong(longValue.Value) : null;

            private static Func<TEnum, long> CompileToLong()
            {
                var parameter = Expression.Parameter(typeof(TEnum), "value");
                return Expression.Lambda<Func<TEnum, long>>(Expression.Convert(parameter, typeof(long)), parameter).Compile();
            }

            private static Func<long, TEnum> CompileFromLong()
            {
                var parameter = Expression.Parameter(typeof(long), "value");
                return Expression.Lambda<Func<long, TEnum>>(Expression.Convert(parameter, typeof(TEnum)), parameter).Compile();
            }
        }

        private static SpannerComplexTypeMapping CreateEnumArrayMapping<TEnum>() where TEnum : struct, Enum
            => CreateArrayMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), EnumConverter<TEnum>.ToLong, EnumConverter<TEnum>.FromLong);

        private static SpannerComplexTypeMapping CreateNullableEnumArrayMapping<TEnum>() where TEnum : struct, Enum
            => CreateArrayMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), EnumConverter<TEnum>.NullableToLong, EnumConverter<TEnum>.NullableFromLong);

        private static SpannerComplexTypeMapping CreateEnumListMapping<TEnum>() where TEnum : struct, Enum
            => CreateListMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), EnumConverter<TEnum>.ToLong, EnumConverter<TEnum>.FromLong);

        private static SpannerComplexTypeMapping CreateNullableEnumListMapping<TEnum>() where TEnum : struct, Enum
            => CreateListMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), EnumConverter<TEnum>.NullableToLong, EnumConverter<TEnum>.NullableFromLong);

        private static SpannerComplexTypeMapping CreateEnumEnumerableMapping<TEnum>() where TEnum : struct, Enum
            => CreateEnumerableMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), EnumConverter<TEnum>.ToLong, EnumConverter<TEnum>.FromLong);

        private static SpannerComplexTypeMapping CreateNullableEnumEnumerableMapping<TEnum>() where TEnum : struct, Enum
            => CreateEnumerableMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64), EnumConverter<TEnum>.NullableToLong, EnumConverter<TEnum>.NullableFromLong);

        /// <summary>
        /// Attempts to determine the element CLR type for a collection or array type.
        /// This enables collection interfaces and types (e.g. <see cref="IEnumerable{T}"/>,
        /// <see cref="IList{T}"/>, <see cref="IReadOnlyList{T}"/>, <see cref="ISet{T}"/>)
        /// to be resolved to their corresponding Spanner ARRAY type mappings.
        /// </summary>
        private static bool TryGetElementType(Type type, out Type elementType)
        {
            if (type.IsGenericTypeDefinition || type.ContainsGenericParameters)
            {
                elementType = null;
                return false;
            }

            // String implements IEnumerable<char> and byte[] implements IEnumerable<byte>,
            // but in Spanner these map to the scalar types STRING and BYTES rather than arrays.
            // Spanner byte arrays are represented as byte[][] or List<byte[]>.
            if (type == typeof(string) || type == typeof(byte[]))
            {
                elementType = null;
                return false;
            }

            // For one-dimensional array types (e.g. T[]), retrieve the element type directly.
            // Multi-dimensional rectangular arrays (e.g. T[,]) are not supported as Spanner arrays.
            if (type.IsArray)
            {
                if (type.GetArrayRank() != 1)
                {
                    elementType = null;
                    return false;
                }
                elementType = type.GetElementType();
                return elementType != null;
            }

            // Non-collection types do not implement IEnumerable. Guard here to avoid
            // calling type.GetInterfaces(), which allocates a Type[] array on each call.
            if (!typeof(IEnumerable).IsAssignableFrom(type))
            {
                elementType = null;
                return false;
            }

            // For the generic IEnumerable<T> interface itself, extract the type argument.
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                var genericArguments = type.GenericTypeArguments;
                if (genericArguments.Length > 0)
                {
                    elementType = genericArguments[0];
                    return true;
                }
            }

            // For types implementing IEnumerable<T> (e.g. IList<T>, IReadOnlyList<T>,
            // ICollection<T>, ISet<T>, HashSet<T>), inspect implemented interfaces.
            foreach (var interfaceType in type.GetInterfaces())
            {
                if (interfaceType.IsGenericType && interfaceType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    var genericArguments = interfaceType.GenericTypeArguments;
                    if (genericArguments.Length > 0)
                    {
                        elementType = genericArguments[0];
                        return true;
                    }
                }
            }

            elementType = null;
            return false;
        }
    }
}
