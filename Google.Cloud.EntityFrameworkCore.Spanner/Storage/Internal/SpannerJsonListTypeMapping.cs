// Copyright 2021, Google LLC
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
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text.Json;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    internal class SpannerJsonListTypeMapping : RelationalTypeMapping
    {
        private static readonly ValueConverter s_converter = new ValueConverter<List<JsonDocument>, List<string>>(
            v => v.Select(jd => jd == null ? null : jd.RootElement.ToString()).ToList(),
            v => v.Select(s => s == null ? null : JsonDocument.Parse(s, SpannerJsonTypeMapping.JsonOptions)).ToList());

        public SpannerJsonListTypeMapping()
            : base(new RelationalTypeMappingParameters(
                   new CoreTypeMappingParameters(typeof(List<JsonDocument>), s_converter),
                   "ARRAY<JSON>"))
        { }

        protected SpannerJsonListTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters) { }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new SpannerJsonListTypeMapping(parameters);

        protected override void ConfigureParameter(DbParameter parameter)
        {
            // This key step will configure our SpannerParameter with this complex type, which will result in
            // the proper type conversions when the requests go out.

            if (parameter is Google.Cloud.Spanner.DataProvider.SpannerParameter spannerDriverParameter)
            {
                base.ConfigureParameter(parameter);
                spannerDriverParameter.SpannerParameterType = SpannerArrayTypes.SArrayOfJsonType;
            }
            else
            {
                if (!(parameter is SpannerParameter))
                    throw new ArgumentException(
                        $"Spanner-specific type mapping {GetType().Name} being used with non-Spanner parameter type {parameter.GetType().Name}");

                base.ConfigureParameter(parameter);
                if (parameter is SpannerParameter spannerParameter)
                {
                    spannerParameter.SpannerDbType = SpannerDbType.ArrayOf(SpannerDbType.Json);
                }
            }
        }
        
        protected override string GenerateNonNullSqlLiteral(object value)
        {
            if (value is IEnumerable<JsonDocument> values)
            {
                return values.Select(jd => $"'{jd.RootElement.ToString()}'").ToString();
            }
            if (value is IEnumerable<string> stringValues)
            {
                return stringValues.Select(s => $"'{s}'").ToString();
            }
            throw new ArgumentException($"{value} is not valid for database type ARRAY<JSON>");
        }
    }
}
