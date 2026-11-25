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
using IEnumerable = System.Collections.IEnumerable;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerJsonArrayTypeMapping : RelationalTypeMapping
    {
        private static readonly ValueConverter s_converter = new ValueConverter<JsonDocument[], string[]>(
            documents => documents == null ? null : documents.Select(document => document == null ? null : document.RootElement.GetRawText()).ToArray(),
            strings => strings == null ? null : strings.Select(json => json == null ? null : JsonDocument.Parse(json, SpannerJsonTypeMapping.JsonOptions)).ToArray());

        public static SpannerJsonArrayTypeMapping Default { get; } = new();

        public SpannerJsonArrayTypeMapping()
            : base(new RelationalTypeMappingParameters(
                   new CoreTypeMappingParameters(typeof(JsonDocument[]), s_converter),
                   "ARRAY<JSON>", StoreTypePostfix.None, System.Data.DbType.Object))
        { }

        protected SpannerJsonArrayTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters) { }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new SpannerJsonArrayTypeMapping(parameters);

        protected override void ConfigureParameter(DbParameter parameter)
        {
            // This key step will configure our SpannerParameter with this complex type, which will result in
            // the proper type conversions when the requests go out.

            if (!(parameter is SpannerParameter spannerParameter))
                throw new ArgumentException($"Spanner-specific type mapping {GetType().Name} being used with non-Spanner parameter type {parameter.GetType().Name}");

            base.ConfigureParameter(parameter);
            spannerParameter.SpannerDbType = SpannerDbType.ArrayOf(SpannerDbType.Json);
        }

        public override string GenerateSqlLiteral(object value)
            => value == null ? "NULL" : GenerateNonNullSqlLiteral(value);

        internal static string GenerateJsonArrayLiteral(object value)
        {
            if (value is IEnumerable enumerable and not string and not byte[])
            {
                var elements = new List<string>();
                foreach (var item in enumerable)
                {
                    elements.Add(SpannerJsonTypeMapping.GenerateJsonLiteral(item));
                }

                return $"ARRAY<JSON>[{string.Join(", ", elements)}]";
            }
            throw new ArgumentException($"{value} is not valid for database type ARRAY<JSON>");
        }

        protected override string GenerateNonNullSqlLiteral(object value)
            => GenerateJsonArrayLiteral(value);
    }
}
