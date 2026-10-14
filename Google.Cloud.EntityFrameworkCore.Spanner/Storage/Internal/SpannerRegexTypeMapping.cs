// Copyright 2026, Google Inc. All rights reserved.
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
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Data.Common;
using System.Text.RegularExpressions;
using DataDbType = System.Data.DbType;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerRegexTypeMapping : RelationalTypeMapping
    {
        private static readonly ValueConverter s_converter = new ValueConverter<Regex, string>(
            regex => regex.ToString(),
            pattern => new Regex(pattern));

        private static readonly ValueComparer<Regex> s_comparer = new(
            (regex1, regex2) => (regex1 == null && regex2 == null) || (regex1 != null && regex2 != null && regex1.ToString() == regex2.ToString() && regex1.Options == regex2.Options),
            regex => HashCode.Combine(regex.ToString(), regex.Options),
            regex => regex == null ? null : new Regex(regex.ToString(), regex.Options));

        public static SpannerRegexTypeMapping Default { get; } = new();

        public SpannerRegexTypeMapping()
            : base(new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(Regex),
                    s_converter,
                    s_comparer),
                SpannerDbType.String.ToString(),
                StoreTypePostfix.None,
                DataDbType.String))
        {
        }

        protected SpannerRegexTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters)
        {
        }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new SpannerRegexTypeMapping(parameters);

        protected override void ConfigureParameter(DbParameter parameter)
        {
            if (parameter is SpannerParameter spannerParameter)
            {
                spannerParameter.SpannerDbType = SpannerDbType.String;
            }
            base.ConfigureParameter(parameter);
        }

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            var pattern = value is Regex regex ? regex.ToString() : value.ToString();
            return $"'{pattern.Replace("\\", "\\\\").Replace("'", "\\'").Replace("\r", "\\r").Replace("\n", "\\n")}'";
        }
    }
}
