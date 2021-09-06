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
using System;
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
    internal class SpannerDateTimeMethodTranslator : IMethodCallTranslator
    {
        private static readonly MethodInfo s_addYearsMethodInfo
            = typeof(SpannerDate).GetRuntimeMethod(nameof(SpannerDate.AddYears), new[] { typeof(int) });

        private static readonly MethodInfo s_addMonthsMethodInfo
            = typeof(SpannerDate).GetRuntimeMethod(nameof(SpannerDate.AddMonths), new[] { typeof(int) });

        private static readonly MethodInfo s_addDaysMethodInfo
            = typeof(SpannerDate).GetRuntimeMethod(nameof(SpannerDate.AddDays), new[] { typeof(int) });

        private static readonly MethodInfo s_dateTimeAddDaysMethodInfo
            = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddDays), new[] { typeof(double) });

        private static readonly MethodInfo s_addHoursMethodInfo
            = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddHours), new[] { typeof(double) });

        private static readonly MethodInfo s_addMinutesMethodInfo
            = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMinutes), new[] { typeof(double) });

        private static readonly MethodInfo s_addSecondsMethodInfo
            = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddSeconds), new[] { typeof(double) });

        private static readonly MethodInfo s_addMillisecondsMethodInfo
            = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddMilliseconds), new[] { typeof(double) });

        private static readonly MethodInfo s_addTicksMethodInfo
            = typeof(DateTime).GetRuntimeMethod(nameof(DateTime.AddTicks), new[] { typeof(long) });

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerDateTimeMethodTranslator([NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public virtual SqlExpression Translate(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
        {
            if (s_addYearsMethodInfo.Equals(method) && IsValidDate(instance))
            {
                return TranslateAddDateInterval(instance, arguments, "YEAR");
            }
            if (s_addMonthsMethodInfo.Equals(method) && IsValidDate(instance))
            {
                return TranslateAddDateInterval(instance, arguments, "MONTH");
            }
            // Adding INTERVAL x DAY is allowed for both DATE and TIMESTAMP.
            if (s_addDaysMethodInfo.Equals(method) && IsValidDate(instance))
            {
                return TranslateAddDateInterval(instance, arguments, "DAY");
            }
            if (s_dateTimeAddDaysMethodInfo.Equals(method) && IsValidTimestamp(instance))
            {
                return TranslateAddTimestampInterval(instance, arguments, "DAY");
            }
            if (s_addHoursMethodInfo.Equals(method) && IsValidTimestamp(instance))
            {
                return TranslateAddTimestampInterval(instance, arguments, "HOUR");
            }
            if (s_addMinutesMethodInfo.Equals(method) && IsValidTimestamp(instance))
            {
                return TranslateAddTimestampInterval(instance, arguments, "MINUTE");
            }
            if (s_addSecondsMethodInfo.Equals(method) && IsValidTimestamp(instance))
            {
                return TranslateAddTimestampInterval(instance, arguments, "SECOND");
            }
            if (s_addMillisecondsMethodInfo.Equals(method) && IsValidTimestamp(instance))
            {
                return TranslateAddTimestampInterval(instance, arguments, "MILLISECOND");
            }
            if (s_addTicksMethodInfo.Equals(method) && IsValidTimestamp(instance))
            {
                return TranslateAddTimestampInterval(instance, arguments, "NANOSECOND", 100L);
            }
            return null;
        }

        private SqlExpression TranslateAddDateInterval(SqlExpression instance, IReadOnlyList<SqlExpression> arguments, string interval)
        {
            return TranslateOneArgFunction("DATE_ADD", instance, new SpannerIntervalExpression(_sqlExpressionFactory, _sqlExpressionFactory.ApplyDefaultTypeMapping(arguments[0]), interval), typeof(SpannerDate));
        }

        private SqlExpression TranslateAddTimestampInterval(SqlExpression instance, IReadOnlyList<SqlExpression> arguments, string interval, long multiplier = 1L)
        {
            return TranslateOneArgFunction("TIMESTAMP_ADD", instance, new SpannerIntervalExpression(_sqlExpressionFactory, GetFirstArgumentAsInt64(arguments, multiplier), interval), typeof(DateTime));
        }

        private bool IsValidDate(SqlExpression instance)
        {
            var typed = _sqlExpressionFactory.ApplyDefaultTypeMapping(instance);
            return typed?.TypeMapping != null && typed.TypeMapping.StoreTypeNameBase == "DATE";
        }

        private bool IsValidTimestamp(SqlExpression instance)
        {
            var typed = _sqlExpressionFactory.ApplyDefaultTypeMapping(instance);
            return typed?.TypeMapping != null && typed.TypeMapping.StoreTypeNameBase == "TIMESTAMP";
        }

        private SqlExpression GetFirstArgumentAsInt64(IReadOnlyList<SqlExpression> arguments, long multiplier)
        {
            SqlExpression value = _sqlExpressionFactory.ApplyDefaultTypeMapping(arguments[0]);
            if (value.TypeMapping != null && value.TypeMapping.StoreTypeNameBase == "FLOAT64")
            {
                value = _sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Function("CAST", new[] { value, _sqlExpressionFactory.Fragment("INT64") }, typeof(long)));
            }
            if (multiplier != 1L)
            {
                value = _sqlExpressionFactory.Multiply(_sqlExpressionFactory.Constant(multiplier), value, value.TypeMapping);
            }
            return value;
        }

        private SqlExpression TranslateOneArgFunction(string function, SqlExpression instance, SqlExpression arg, System.Type returnType)
        {
            return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                _sqlExpressionFactory.Function(
                function,
                new[] { instance, arg },
                returnType));
        }
    }
}
