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
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Data;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    public class SpannerTypeMappingSource : RelationalTypeMappingSource
    {
        internal const int StringMax = 2621440;
        internal const int BytesMax = 10485760;

        private static readonly BoolTypeMapping s_bool
            = new SpannerBoolTypeMapping(SpannerDbType.Bool.ToString());

        private static readonly SpannerDateTypeMapping s_date = new SpannerDateTypeMapping();

        private static readonly SpannerTimestampTypeMapping s_datetime = new SpannerTimestampTypeMapping();

        private static readonly StringTypeMapping s_defaultString
            = new SpannerStringTypeMapping(SpannerDbType.String.ToString(), unicode: true, sqlDbType: SpannerDbType.String);

        private static readonly DoubleTypeMapping s_double = new SpannerDoubleTypeMapping();

        private static readonly IntTypeMapping s_int = new IntTypeMapping(SpannerDbType.Int64.ToString(), DbType.Int32);

        private static readonly LongTypeMapping s_long
            = new LongTypeMapping(SpannerDbType.Int64.ToString(), DbType.Int64);

        private static readonly SpannerNumericTypeMapping s_decimal
            = new SpannerNumericTypeMapping(SpannerDbType.Numeric.ToString(), 29, 9, StoreTypePostfix.PrecisionAndScale);

        private static readonly GuidTypeMapping s_guid
            = new GuidTypeMapping(SpannerDbType.String.ToString(), DbType.String);

        private static readonly ByteArrayTypeMapping s_byte
            = new ByteArrayTypeMapping(SpannerDbType.Bytes.ToString(), DbType.Binary);

        private static readonly SpannerComplexTypeMapping s_byteArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bytes));

        private static readonly SpannerComplexTypeMapping s_stringArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.String));

        private static readonly SpannerComplexTypeMapping s_boolArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Bool));

        private static readonly SpannerComplexTypeMapping s_doubleArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Float64));

        private static readonly SpannerComplexTypeMapping s_longArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Int64));

        private static readonly SpannerComplexTypeMapping s_dateArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Date));

        private static readonly SpannerComplexTypeMapping s_timestampArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Timestamp));

        private static readonly SpannerComplexTypeMapping s_numericArray
            = new SpannerComplexTypeMapping(SpannerDbType.ArrayOf(SpannerDbType.Numeric));

        private readonly Dictionary<System.Type, RelationalTypeMapping> s_clrTypeMappings;

        private readonly Dictionary<string, RelationalTypeMapping> s_storeTypeMappings;


        public SpannerTypeMappingSource(
            TypeMappingSourceDependencies dependencies,
            RelationalTypeMappingSourceDependencies relationalDependencies)
            : base(dependencies, relationalDependencies)
        {
            s_clrTypeMappings
                = new Dictionary<System.Type, RelationalTypeMapping>
                {
                {typeof(short), s_long},
                {typeof(int), s_int},
                {typeof(long), s_long},
                {typeof(decimal), s_decimal},
                {typeof(uint), s_long},
                {typeof(bool), s_bool},
                {typeof(DateTime), s_datetime},
                {typeof(float), s_double},
                {typeof(double), s_double},
                {typeof(string), s_defaultString},
                {typeof(string[]), s_stringArray},
                {typeof(bool[]), s_boolArray},
                {typeof(double[]), s_doubleArray},
                {typeof(long[]), s_longArray},
                // TODO: Figure out how to register this {typeof(DateTime[]), s_dateArray},
                {typeof(DateTime[]), s_timestampArray},
                {typeof(decimal[]), s_numericArray},
                {typeof(Guid), s_guid},
                {typeof(byte[]), s_byte}
                };

            s_storeTypeMappings = new Dictionary<string, RelationalTypeMapping>
            {
                {SpannerDbType.Bool.ToString(), s_bool},
                {SpannerDbType.Bytes.ToString(), s_byte},
                {SpannerDbType.Date.ToString(), s_date},
                {SpannerDbType.Float64.ToString(), s_double},
                {SpannerDbType.Int64.ToString(), s_long},
                {SpannerDbType.Timestamp.ToString(), s_datetime},
                {SpannerDbType.String.ToString(), s_defaultString},
                {SpannerDbType.Numeric.ToString(), s_decimal},
                {SpannerDbType.Unspecified.ToString(), null},
                {"ARRAY<BOOL>", s_boolArray},
                {"ARRAY<BYTES", s_byteArray},
                {"ARRAY<DATE>", s_dateArray},
                {"ARRAY<FLOAT64>", s_doubleArray},
                {"ARRAY<INT64>", s_longArray},
                {"ARRAY<STRING", s_stringArray},
                {"ARRAY<TIMESTAMP>", s_timestampArray},
                {"ARRAY<NUMERIC>", s_numericArray}
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
                    return clrType == null || mapping.ClrType == clrType ? mapping : null;
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
    }
}
