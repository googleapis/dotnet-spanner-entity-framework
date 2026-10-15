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

using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System;
using System.Collections.Generic;
using System.Reflection;
namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    using Type = System.Type;
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    internal class SpannerDateTimeMemberTranslator : IMemberTranslator
    {
        private static readonly Dictionary<string, string> s_datePartMapping
            = new Dictionary<string, string>
            {
                { nameof(DateTime.Year), "YEAR" },
                { nameof(DateTime.Month), "MONTH" },
                { nameof(DateTime.Day), "DAY" },
                { nameof(DateTime.DayOfYear), "DAYOFYEAR" },
                { nameof(DateTime.DayOfWeek), "DAYOFWEEK" },
                { nameof(DateTime.Hour), "HOUR" },
                { nameof(DateTime.Minute), "MINUTE" },
                { nameof(DateTime.Second), "SECOND" },
                { nameof(DateTime.Millisecond), "MILLISECOND" },
                { nameof(DateTime.Microsecond), "MICROSECOND" },
                { nameof(DateTime.Nanosecond), "NANOSECOND" },
            };

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerDateTimeMemberTranslator(
            [NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public virtual SqlExpression Translate(
            SqlExpression instance,
            MemberInfo member,
            Type returnType,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            var declaringType = member.DeclaringType;

            if (declaringType == typeof(DateTime))
            {
                var memberName = member.Name;

                if (instance == null)
                {
                    if (memberName is nameof(DateTime.UtcNow) or nameof(DateTime.Now))
                    {
                        var currentTimestamp = _sqlExpressionFactory.Function(
                            "CURRENT_TIMESTAMP",
                            Array.Empty<SqlExpression>(),
                            nullable: false,
                            argumentsPropagateNullability: Array.Empty<bool>(),
                            returnType);

                        return _sqlExpressionFactory.ApplyDefaultTypeMapping(currentTimestamp);
                    }

                    if (memberName == nameof(DateTime.Today))
                    {
                        var currentTimestamp = _sqlExpressionFactory.Function(
                            "CURRENT_TIMESTAMP",
                            Array.Empty<SqlExpression>(),
                            nullable: false,
                            argumentsPropagateNullability: Array.Empty<bool>(),
                            returnType);

                        var timestampTrunc = _sqlExpressionFactory.Function(
                            "TIMESTAMP_TRUNC",
                            new[]
                            {
                                currentTimestamp,
                                _sqlExpressionFactory.Fragment("DAY"),
                                _sqlExpressionFactory.Constant("UTC")
                            },
                            nullable: false,
                            argumentsPropagateNullability: new[] { false, false, false },
                            returnType);

                        return _sqlExpressionFactory.ApplyDefaultTypeMapping(timestampTrunc);
                    }

                    return null;
                }

                if (memberName == nameof(DateTime.Date))
                {
                    var timestampTrunc = _sqlExpressionFactory.Function(
                        "TIMESTAMP_TRUNC",
                        new[]
                        {
                            instance,
                            _sqlExpressionFactory.Fragment("DAY"),
                            _sqlExpressionFactory.Constant("UTC")
                        },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true, false, false },
                        returnType);

                    return _sqlExpressionFactory.ApplyDefaultTypeMapping(timestampTrunc);
                }

                if (s_datePartMapping.TryGetValue(memberName, out var datePart))
                {
                    var extract = _sqlExpressionFactory.Function(
                        "EXTRACT",
                        new[] { new SpannerTimestampExtractExpression(_sqlExpressionFactory, instance, datePart) },
                        /* return value is nullable = */true,
                        /* arguments that propagate nullability = */new[] { true },
                        returnType);
                    if (datePart == "DAYOFWEEK")
                    {
                        // Cloud Spanner is 1-based, .NET is 0-based.
                        return _sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Subtract(extract, _sqlExpressionFactory.Constant(1)));
                    }
                    if (datePart is "MICROSECOND" or "NANOSECOND")
                    {
                        return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                            _sqlExpressionFactory.Function(
                                "MOD",
                                new[] { extract, _sqlExpressionFactory.Constant(1000) },
                                nullable: true,
                                argumentsPropagateNullability: new[] { true, false },
                                returnType));
                    }
                    return _sqlExpressionFactory.ApplyDefaultTypeMapping(extract);
                }
            }
            return null;
        }
    }
}
