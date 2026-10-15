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
using System;
using System.Collections.Generic;
using System.Data;
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

        private readonly Dictionary<Type, RelationalTypeMapping> _clrTypeMappings;

        private readonly Dictionary<string, RelationalTypeMapping> _storeTypeMappings;

        private readonly Dictionary<string, RelationalTypeMapping> _arrayTypeMappings;

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
                {"ARRAY<BYTES", s_byteList},
                {"ARRAY<DATE>", s_nullableDateList},
                {"ARRAY<FLOAT32>", s_nullableFloatList},
                {"ARRAY<FLOAT64>", s_nullableDoubleList},
                {"ARRAY<INT64>", s_nullableLongList},
                {"ARRAY<STRING", s_stringList},
                {"ARRAY<TIMESTAMP>", s_nullableTimestampList},
                {"ARRAY<NUMERIC>", s_nullableNumericList},
                {"ARRAY<JSON>", s_jsonList}
            };

            _arrayTypeMappings = new Dictionary<string, RelationalTypeMapping>(StringComparer.OrdinalIgnoreCase)
            {
                {"ARRAY<BOOL>", s_nullableBoolArray},
                {"ARRAY<BYTES", s_byteArray},
                {"ARRAY<DATE>", s_nullableDateArray},
                {"ARRAY<FLOAT32>", s_nullableFloatArray},
                {"ARRAY<FLOAT64>", s_nullableDoubleArray},
                {"ARRAY<INT64>", s_nullableLongArray},
                {"ARRAY<STRING", s_stringArray},
                {"ARRAY<TIMESTAMP>", s_nullableTimestampArray},
                {"ARRAY<NUMERIC>", s_nullableNumericArray},
                {"ARRAY<JSON>", s_jsonArray}
            };

            _elementClrTypeMappings = new Dictionary<Type, RelationalTypeMapping>
            {
                {typeof(int), s_intArray},
                {typeof(int?), s_nullableIntArray},
                {typeof(long), s_longArray},
                {typeof(long?), s_nullableLongArray},
                {typeof(decimal), s_decimalArray},
                {typeof(decimal?), s_nullableDecimalArray},
                {typeof(SpannerNumeric), s_numericArray},
                {typeof(SpannerNumeric?), s_nullableNumericArray},
                {typeof(string), s_stringArray},
                {typeof(bool), s_boolArray},
                {typeof(bool?), s_nullableBoolArray},
                {typeof(float), s_floatArray},
                {typeof(float?), s_nullableFloatArray},
                {typeof(double), s_doubleArray},
                {typeof(double?), s_nullableDoubleArray},
                {typeof(DateTime), s_timestampArray},
                {typeof(DateTime?), s_nullableTimestampArray},
                {typeof(SpannerDate), s_dateArray},
                {typeof(SpannerDate?), s_nullableDateArray},
                {typeof(JsonDocument), s_jsonArray},
                {typeof(byte[]), s_byteArray},
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

            // Handle JsonTypePlaceholder type for EF Core 8's .ToJson() owned entities.
            // JsonTypePlaceholder is an internal type in EF Core, so we detect it by name.
            if (clrType != null && clrType.Name == "JsonTypePlaceholder")
            {
                // Return the structural JSON type mapping which uses JSON '...' literal syntax
                return s_structuralJson;
            }

            if (storeTypeName != null)
            {
                if (storeTypeName.Equals("JSON", StringComparison.InvariantCultureIgnoreCase) &&
                    clrType == typeof(string))
                {
                    return s_jsonAsString;
                }

                if (_storeTypeMappings.TryGetValue(storeTypeName, out var mapping)
                    || _storeTypeMappings.TryGetValue(storeTypeNameBase, out mapping))
                {
                    if (clrType == null
                        || mapping.ClrType == clrType
                        || mapping.Converter?.ProviderClrType == clrType)
                    {
                        return mapping;
                    }

                    if (_arrayTypeMappings.TryGetValue(storeTypeName, out var arrayMapping)
                        || _arrayTypeMappings.TryGetValue(storeTypeNameBase, out arrayMapping))
                    {
                        if (arrayMapping.ClrType == clrType
                            || arrayMapping.Converter?.ProviderClrType == clrType
                            || clrType.IsAssignableFrom(arrayMapping.ClrType))
                        {
                            return arrayMapping;
                        }
                    }
                }

                if (TryFindConverterMapping(storeTypeName, clrType, out mapping))
                {
                    return mapping;
                }
            }

            if (clrType != null)
            {
                if (_clrTypeMappings.TryGetValue(clrType, out var mapping))
                {
                    return mapping;
                }

                if (TryGetElementType(clrType, out var elementType)
                    && _elementClrTypeMappings.TryGetValue(elementType, out mapping))
                {
                    return mapping;
                }
            }

            return null;
        }

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

        private bool TryFindConverterMapping(string storeType, Type clrType, out RelationalTypeMapping mapping)
        {
            foreach (var m in _clrTypeMappings.Values)
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
