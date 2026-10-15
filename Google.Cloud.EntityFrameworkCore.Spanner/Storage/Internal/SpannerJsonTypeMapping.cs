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
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Data.Common;
using System.Text.Json;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerJsonTypeMapping : RelationalTypeMapping
    {
        public static readonly JsonDocumentOptions JsonOptions = new()
        { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip, MaxDepth = 100 };
        private static readonly ValueConverter s_converter = new ValueConverter<JsonDocument, string>(
            v => v.RootElement.ToString(),
            v => JsonDocument.Parse(v, JsonOptions));

        public static SpannerJsonTypeMapping Default { get; } = new();

        public SpannerJsonTypeMapping()
            : base(new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(JsonDocument), s_converter),
                "JSON", StoreTypePostfix.None, System.Data.DbType.Object))
        { }

        protected SpannerJsonTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters) { }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new SpannerJsonTypeMapping(parameters);

        protected override void ConfigureParameter(DbParameter parameter)
        {
            ((SpannerParameter)parameter).SpannerDbType = SpannerDbType.Json;
            base.ConfigureParameter(parameter);
        }

        internal static string EscapeJsonLiteral(string literal)
        {
            if (string.IsNullOrEmpty(literal))
            {
                return literal ?? string.Empty;
            }

            return literal
                .Replace(@"\", @"\\")
                .Replace("'", @"\'")
                .Replace("\0", @"\\u0000")
                .Replace("\r", @"\r")
                .Replace("\n", @"\n")
                .Replace("\t", @"\t");
        }

        // Overridden to bypass base RelationalTypeMapping's Converter.ConvertToProvider call,
        // which throws InvalidCastException for string or JsonElement values and strips quotes
        // on scalar string JSON documents via JsonElement.ToString().
        public override string GenerateSqlLiteral(object value)
            => value == null ? "NULL" : GenerateNonNullSqlLiteral(value);

        internal static string GenerateJsonLiteral(object value)
        {
            if (value is JsonDocument jsonDocument)
            {
                return $"JSON '{EscapeJsonLiteral(jsonDocument.RootElement.GetRawText())}'";
            }
            if (value is JsonElement jsonElement)
            {
                return $"JSON '{EscapeJsonLiteral(jsonElement.GetRawText())}'";
            }
            if (value is string stringValue)
            {
                return $"JSON '{EscapeJsonLiteral(stringValue)}'";
            }
            throw new ArgumentException($"{value} is not valid for database type JSON");
        }

        protected override string GenerateNonNullSqlLiteral(object value)
            => GenerateJsonLiteral(value);
    }
}
