// Copyright 2026 Google LLC
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

using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Data.Common;
using DataDbType = System.Data.DbType;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerCharTypeMapping : CharTypeMapping
    {
        public new static SpannerCharTypeMapping Default { get; } = new("STRING(1)", DataDbType.String);

        public SpannerCharTypeMapping(string storeType, DataDbType? dbType = DataDbType.String)
            : base(storeType, dbType ?? DataDbType.String)
        {
        }

        protected SpannerCharTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new SpannerCharTypeMapping(parameters);

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);
            if (parameter is SpannerParameter spannerParameter)
            {
                spannerParameter.SpannerDbType = SpannerDbType.String;
            }
        }

        protected virtual string EscapeSqlLiteral(string literal)
            => SpannerStringTypeMapping.EscapeStringLiteral(literal);

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var characterValue = value is char character ? character.ToString() : value?.ToString() ?? string.Empty;
            return $"'''{EscapeSqlLiteral(characterValue)}'''";
        }
    }
}
