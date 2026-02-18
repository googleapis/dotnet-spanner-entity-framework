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

using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerDateTypeMapping : RelationalTypeMapping
    {
        public static SpannerDateTypeMapping Default { get; } = new();
        
        private static readonly ValueConverter s_converter = new ValueConverter<SpannerDate, DateTime>(
            v => v.ToDateTime(),
            v => SpannerDate.FromDateTime(v));

        public SpannerDateTypeMapping()
            : base(new RelationalTypeMappingParameters(
                   new CoreTypeMappingParameters(typeof(SpannerDate), s_converter),
                   "DATE", StoreTypePostfix.None, System.Data.DbType.Date))
        { }

        protected SpannerDateTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters) { }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new SpannerDateTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
        {
            if (value is DateTime dt)
            {
                return FormattableString.Invariant($"DATE '{dt:yyyy-MM-dd}'");
            }
            if (value is SpannerDate sd)
            {
                return $"DATE '{sd}'";
            }
            throw new ArgumentException($"{value} is not valid for database type DATE");
        }
    }
}
