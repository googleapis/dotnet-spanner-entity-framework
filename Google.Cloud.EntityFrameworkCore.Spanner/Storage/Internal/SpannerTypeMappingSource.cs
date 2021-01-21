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
using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    public class SpannerTypeMappingSource : RelationalTypeMappingSource
    {
        internal const int StringMax = 2621440;
        internal const int BytesMax = 10485760;

        // --- Non-array types ---

        private static readonly BoolTypeMapping s_bool
            = new SpannerBoolTypeMapping(SpannerDbType.Bool.ToString());

        private static readonly SpannerDateTypeMapping s_date = new SpannerDateTypeMapping();

        private static readonly SpannerTimestampTypeMapping s_datetime = new SpannerTimestampTypeMapping();

        private static readonly StringTypeMapping s_defaultString
            = new SpannerStringTypeMapping(SpannerDbType.String.ToString(), unicode: true, sqlDbType: SpannerDbType.String);

        private static readonly SpannerComplexTypeMapping s_float =
            new SpannerComplexTypeMapping(SpannerDbType.Float64, typeof(float));

        private static readonly DoubleTypeMapping s_double = new SpannerDoubleTypeMapping();

        private static readonly SpannerComplexTypeMapping s_int =
            new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(int));

        private static readonly LongTypeMapping s_long
            = new LongTypeMapping(SpannerDbType.Int64.ToString(), DbType.Int64);

        private static readonly SpannerComplexTypeMapping s_uint
            = new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(uint));

        private static readonly SpannerComplexTypeMapping s_short
            = new SpannerComplexTypeMapping(SpannerDbType.Int64, typeof(short));

        private static readonly SpannerNumericTypeMapping s_numeric = new SpannerNumericTypeMapping();

        private static readonly GuidTypeMapping s_guid
            = new GuidTypeMapping(SpannerDbType.String.ToString(), DbType.String);

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
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(string[]));

        private static readonly SpannerComplexTypeMapping s_stringList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String), typeof(List<string>));

        private static readonly SpannerComplexTypeMapping s_nullableBoolArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bool), typeof(bool?[]));
        private static readonly SpannerComplexTypeMapping s_boolArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bool), typeof(bool[]));

        private static readonly SpannerComplexTypeMapping s_nullableBoolList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bool), typeof(List<bool?>));
        private static readonly SpannerComplexTypeMapping s_boolList
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bool), typeof(List<bool>));

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

        private readonly Dictionary<System.Type, RelationalTypeMapping> s_clrTypeMappings;

        private readonly Dictionary<string, RelationalTypeMapping> s_storeTypeMappings;

        private readonly Dictionary<string, RelationalTypeMapping> s_arrayTypeMappings;

        public SpannerTypeMappingSource(
            TypeMappingSourceDependencies dependencies,
            RelationalTypeMappingSourceDependencies relationalDependencies)
            : base(dependencies, relationalDependencies)
        {

            s_clrTypeMappings
                = new Dictionary<System.Type, RelationalTypeMapping>
                {
                    {typeof(short), s_short},
                    {typeof(int), s_int},
                    {typeof(long), s_long},
                    {typeof(decimal), s_numeric},
                    {typeof(SpannerNumeric), s_numeric},
                    {typeof(uint), s_uint},
                    {typeof(bool), s_bool},
                    {typeof(SpannerDate), s_date},
                    {typeof(DateTime), s_datetime},
                    {typeof(float), s_float},
                    {typeof(double), s_double},
                    {typeof(string), s_defaultString},
                    {typeof(Guid), s_guid},
                    {typeof(Regex), s_defaultString },
                    {typeof(byte), s_byte},
                    {typeof(sbyte), s_sbyte},
                    {typeof(ulong), s_ulong},
                    {typeof(ushort), s_ushort},
                    {typeof(byte[]), s_bytes},

                    {typeof(decimal[]), s_numericArray},
                    {typeof(decimal?[]), s_nullableNumericArray},
                    {typeof(SpannerNumeric[]), s_numericArray},
                    {typeof(SpannerNumeric?[]), s_nullableNumericArray},
                    {typeof(List<decimal>), s_numericList},
                    {typeof(List<decimal?>), s_nullableNumericList},
                    {typeof(List<SpannerNumeric>), s_numericList},
                    {typeof(List<SpannerNumeric?>), s_nullableNumericList},
                    {typeof(string[]), s_stringArray},
                    {typeof(List<string>), s_stringList},
                    {typeof(bool[]), s_boolArray},
                    {typeof(bool?[]), s_nullableBoolArray},
                    {typeof(List<bool>), s_boolList},
                    {typeof(List<bool?>), s_nullableBoolList},
                    {typeof(double[]), s_doubleArray},
                    {typeof(double?[]), s_nullableDoubleArray},
                    {typeof(List<double>), s_doubleList},
                    {typeof(List<double?>), s_nullableDoubleList},
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
                    {typeof(byte[][]), s_byteArray},
                    {typeof(List<byte[]>), s_byteList},
                };

            s_storeTypeMappings = new Dictionary<string, RelationalTypeMapping>
            {
                {SpannerDbType.Bool.ToString(), s_bool},
                {SpannerDbType.Bytes.ToString(), s_bytes},
                {SpannerDbType.Date.ToString(), s_date},
                {SpannerDbType.Float64.ToString(), s_double},
                {SpannerDbType.Int64.ToString(), s_long},
                {SpannerDbType.Timestamp.ToString(), s_datetime},
                {SpannerDbType.String.ToString(), s_defaultString},
                {SpannerDbType.Numeric.ToString(), s_numeric},
                {"ARRAY<BOOL>", s_nullableBoolList},
                {"ARRAY<BYTES", s_byteList},
                {"ARRAY<DATE>", s_nullableDateList},
                {"ARRAY<FLOAT64>", s_nullableDoubleList},
                {"ARRAY<INT64>", s_nullableLongList},
                {"ARRAY<STRING", s_stringList},
                {"ARRAY<TIMESTAMP>", s_nullableTimestampList},
                {"ARRAY<NUMERIC>", s_nullableNumericList}
            };

            s_arrayTypeMappings = new Dictionary<string, RelationalTypeMapping>
            {
                {"ARRAY<BOOL>", s_nullableBoolArray},
                {"ARRAY<BYTES", s_byteArray},
                {"ARRAY<DATE>", s_nullableDateArray},
                {"ARRAY<FLOAT64>", s_nullableDoubleArray},
                {"ARRAY<INT64>", s_nullableLongArray},
                {"ARRAY<STRING", s_stringArray},
                {"ARRAY<TIMESTAMP>", s_nullableTimestampArray},
                {"ARRAY<NUMERIC>", s_nullableNumericArray}
            };
        }

        protected override RelationalTypeMapping FindMapping(in RelationalTypeMappingInfo mappingInfo)
            => FindRawMapping(mappingInfo)?.Clone(mappingInfo)
                ?? base.FindMapping(mappingInfo);

        private RelationalTypeMapping FindRawMapping(RelationalTypeMappingInfo mappingInfo)
        {
            var clrType = mappingInfo.ClrType;
            var storeTypeName = mappingInfo.StoreTypeName;
            var storeTypeNameBase = mappingInfo.StoreTypeNameBase;

            if (storeTypeName != null)
            {
                if (s_storeTypeMappings.TryGetValue(storeTypeName, out var mapping)
                    || s_storeTypeMappings.TryGetValue(storeTypeNameBase, out mapping))
                {
                    if (clrType == null
                        || mapping.ClrType == clrType
                        || mapping.Converter?.ProviderClrType == clrType)
                    {
                        return mapping;
                    };

                    if (s_arrayTypeMappings.TryGetValue(storeTypeName, out var arrayMapping)
                    || s_arrayTypeMappings.TryGetValue(storeTypeNameBase, out arrayMapping))
                    {
                        if (clrType == null
                            || arrayMapping.ClrType == clrType
                            || arrayMapping.Converter?.ProviderClrType == clrType)
                        {
                            return arrayMapping;
                        };
                    }
                }

                if (TryFindConverterMapping(storeTypeName, clrType, out mapping))
                {
                    return mapping;
                }
            }

            if (clrType != null)
            {
                if (s_clrTypeMappings.TryGetValue(clrType, out var mapping))
                {
                    return mapping;
                }
            }

            return null;
        }

        private bool TryFindConverterMapping(string storeType, System.Type clrType, out RelationalTypeMapping mapping)
        {
            foreach (var m in s_clrTypeMappings.Values)
            {
                if (m.Converter?.ProviderClrType == clrType && m.StoreType == storeType)
                {
                    mapping = m;
                    return true;
                }
            }
            mapping = null;
            return false;
        }
    }
}
