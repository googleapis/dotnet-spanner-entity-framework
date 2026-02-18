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

using Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal;
using Google.Cloud.Spanner.Data;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Storage;
using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerTimestampTypeMapping : RelationalTypeMapping
    {
        public static SpannerTimestampTypeMapping Default { get; } = new();
        
        public SpannerTimestampTypeMapping() : base(SpannerDbType.Timestamp.ToString(), typeof(DateTime), System.Data.DbType.DateTime) { }

        protected SpannerTimestampTypeMapping(RelationalTypeMappingParameters parameters)
            : base(parameters) { }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new SpannerTimestampTypeMapping(parameters);

        protected override string GenerateNonNullSqlLiteral(object value)
            => FormattableString.Invariant($"TIMESTAMP '{(DateTime)value:yyyy-MM-ddTHH:mm:ss.FFFFFFF}Z'");

        public override string GenerateProviderValueSqlLiteral([CanBeNull] object value)
        {
            if (value is string stringVal)
            {
                if (stringVal == SpannerPendingCommitTimestampColumnModification.PendingCommitTimestampValue)
                {
                    return stringVal;
                }
            }
            return base.GenerateProviderValueSqlLiteral(value);
        }
    }
}
