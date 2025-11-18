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
using System.Data.Common;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// Represents a complex spanner type mapping. This class is used for setting up type conversions.
    /// This class can be used to hold a type mapping for any <see cref="SpannerDbType"/>
    /// </summary>
    internal class SpannerComplexTypeMapping : RelationalTypeMapping
    {
        private static readonly List<SpannerDbType> s_arrayTypes = new List<SpannerDbType>
        {
            SpannerDbType.ArrayOf(SpannerDbType.Bool),
            SpannerDbType.ArrayOf(SpannerDbType.Bytes),
            SpannerDbType.ArrayOf(SpannerDbType.Date),
            SpannerDbType.ArrayOf(SpannerDbType.Float64),
            SpannerDbType.ArrayOf(SpannerDbType.Int64),
            SpannerDbType.ArrayOf(SpannerDbType.Json),
            SpannerDbType.ArrayOf(SpannerDbType.Numeric),
            SpannerDbType.ArrayOf(SpannerDbType.String),
            SpannerDbType.ArrayOf(SpannerDbType.Timestamp),
        };

        private readonly SpannerDbType _complexType;
        private readonly System.Type _clrType;
        internal readonly bool IsArrayType;

        public SpannerComplexTypeMapping(SpannerDbType complexType, System.Type clrType, bool unicode = false, int? size = null)
            : base(complexType.ToString(), clrType, unicode: unicode, size: size)
        {
            _complexType = complexType;
            _clrType = clrType;
            IsArrayType = s_arrayTypes.Contains(complexType);
        }

        private SpannerComplexTypeMapping(RelationalTypeMappingParameters parameters, SpannerDbType complexType, System.Type clrType)
            : base(parameters)
        {
            _complexType = complexType;
            _clrType = clrType;
            IsArrayType = s_arrayTypes.Contains(complexType);
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        {
            return new SpannerComplexTypeMapping(parameters, _complexType, _clrType);
        }

        protected override void ConfigureParameter(DbParameter parameter)
        {
            // This key step will configure our SpannerParameter with this complex type, which will result in
            // the proper type conversions when the requests go out.

            if (parameter is SpannerParameter spannerParameter)
            {
                base.ConfigureParameter(parameter);
                if (!IsArrayType && Size.HasValue && Size.Value > 0)
                {
                    parameter.Size = Size.Value;
                }
                spannerParameter.SpannerDbType = _complexType;
            }
            else if (parameter is Google.Cloud.Spanner.DataProvider.SpannerParameter)
            {
                base.ConfigureParameter(parameter);
            }
            else
            {
                throw new ArgumentException(
                    $"Spanner-specific type mapping {GetType().Name} being used with non-Spanner parameter type {parameter.GetType().Name}");
            }
        }
    }
}
