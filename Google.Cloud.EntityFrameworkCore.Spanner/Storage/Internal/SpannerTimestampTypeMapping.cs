﻿// Copyright 2020, Google Inc. All rights reserved.
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
    internal class SpannerTimestampTypeMapping : RelationalTypeMapping
    {
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
