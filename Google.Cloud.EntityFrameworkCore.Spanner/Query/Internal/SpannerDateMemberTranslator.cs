// Copyright 2021 Google LLC
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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System.Collections.Generic;
using System.Reflection;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerDateMemberTranslator : IMemberTranslator
    {
        private static readonly Dictionary<string, string> _datePartMapping
            = new Dictionary<string, string>
            {
                { nameof(SpannerDate.Year), "YEAR" },
                { nameof(SpannerDate.Month), "MONTH" },
                { nameof(SpannerDate.Day), "DAY" },
                { nameof(SpannerDate.DayOfYear), "DAYOFYEAR" },
                { nameof(SpannerDate.DayOfWeek), "DAYOFWEEK" },
            };

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerDateMemberTranslator(
            [NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public virtual SqlExpression Translate(
            SqlExpression instance,
            MemberInfo member,
            System.Type returnType)
        {
            var declaringType = member.DeclaringType;

            if (declaringType == typeof(SpannerDate))
            {
                var memberName = member.Name;

                if (_datePartMapping.TryGetValue(memberName, out var datePart))
                {
                    var extract = _sqlExpressionFactory.Function(
                        "EXTRACT",
                        new[] { new SpannerDateExtractExpression(_sqlExpressionFactory, instance, datePart) },
                        returnType);
                    if (datePart == "DAYOFWEEK")
                    {
                        // Cloud Spanner is 1-based, .NET is 0-based.
                        return _sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Subtract(extract, _sqlExpressionFactory.Constant(1)));
                    }
                    return _sqlExpressionFactory.ApplyDefaultTypeMapping(extract);
                }
            }
            return null;
        }
    }
}
