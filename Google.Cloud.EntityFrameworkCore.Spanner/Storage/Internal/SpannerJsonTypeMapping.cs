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
    public class SpannerJsonTypeMapping : RelationalTypeMapping
    {
        internal static readonly JsonDocumentOptions JsonOptions = new JsonDocumentOptions
            { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip, MaxDepth = 100 };
        private static readonly ValueConverter s_converter = new ValueConverter<JsonDocument, string>(
            v => v.RootElement.ToString(),
            v => JsonDocument.Parse(v, JsonOptions));
        
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
        
        protected override string GenerateNonNullSqlLiteral(object value)
        {
            if (value is JsonDocument jd)
            {
                return $"'{jd.RootElement.ToString()}'";
            }
            if (value is string s)
            {
                return $"'{s}'";
            }
            throw new ArgumentException($"{value} is not valid for database type JSON");
        }
    }
}
