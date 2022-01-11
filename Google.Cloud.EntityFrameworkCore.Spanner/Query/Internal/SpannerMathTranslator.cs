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

using Google.Cloud.Spanner.V1;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    internal class SpannerMathTranslator : IMethodCallTranslator
    {
        private static readonly Dictionary<MethodInfo, string> s_supportedMethods = new Dictionary<MethodInfo, string>
        {
            { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(double) }), "ABS" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(float) }), "ABS" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(decimal) }), "ABS" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(int) }), "ABS" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(long) }), "ABS" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(sbyte) }), "ABS" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), new[] { typeof(short) }), "ABS" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Max), new[] { typeof(byte), typeof(byte) }), "GREATEST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Max), new[] { typeof(double), typeof(double) }), "GREATEST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Max), new[] { typeof(float), typeof(float) }), "GREATEST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Max), new[] { typeof(decimal), typeof(decimal) }), "GREATEST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Max), new[] { typeof(int), typeof(int) }), "GREATEST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Max), new[] { typeof(long), typeof(long) }), "GREATEST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Max), new[] { typeof(sbyte), typeof(sbyte) }), "GREATEST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Max), new[] { typeof(short), typeof(short) }), "GREATEST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Max), new[] { typeof(uint), typeof(uint) }), "GREATEST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Max), new[] { typeof(ushort), typeof(ushort) }), "GREATEST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Min), new[] { typeof(byte), typeof(byte) }), "LEAST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Min), new[] { typeof(double), typeof(double) }), "LEAST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Min), new[] { typeof(float), typeof(float) }), "LEAST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Min), new[] { typeof(decimal), typeof(decimal) }), "LEAST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Min), new[] { typeof(int), typeof(int) }), "LEAST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Min), new[] { typeof(long), typeof(long) }), "LEAST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Min), new[] { typeof(sbyte), typeof(sbyte) }), "LEAST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Min), new[] { typeof(short), typeof(short) }), "LEAST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Min), new[] { typeof(uint), typeof(uint) }), "LEAST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Min), new[] { typeof(ushort), typeof(ushort) }), "LEAST" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Round), new[] { typeof(double), typeof(MidpointRounding) }), "ROUND" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Round), new[] { typeof(double), typeof(int), typeof(MidpointRounding) }), "ROUND" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Round), new[] { typeof(decimal), typeof(MidpointRounding) }), "ROUND" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Round), new[] { typeof(decimal), typeof(int), typeof(MidpointRounding) }), "ROUND" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Ceiling), new[] { typeof(double) }), "CEIL" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Ceiling), new[] { typeof(decimal) }), "CEIL" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Floor), new[] { typeof(double) }), "FLOOR" },
            { typeof(Math).GetRuntimeMethod(nameof(Math.Floor), new[] { typeof(decimal) }), "FLOOR" },
        };

        private static readonly MethodInfo s_spannerNumericToDecimalMethodInfo
            = typeof(SpannerNumeric).GetRuntimeMethod(nameof(SpannerNumeric.ToDecimal), new[] { typeof(LossOfPrecisionHandling) });

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerMathTranslator([NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public virtual SqlExpression Translate(
           SqlExpression instance,
           MethodInfo method,
           IReadOnlyList<SqlExpression> arguments,
           IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            if (s_supportedMethods.TryGetValue(method, out var sqlFunctionName))
            {
                if (sqlFunctionName == "ROUND")
                {
                    // Only rounding mode AwayFromZero can be evaluated server side.
                    // Trying to use a different rounding mode in a query will fail.
                    if (arguments[arguments.Count - 1] is SqlConstantExpression c
                        && c.Value is MidpointRounding mode
                        && mode == MidpointRounding.AwayFromZero)
                    {
                        arguments = arguments.ToList().GetRange(0, arguments.Count - 1);
                    }
                    else
                    {
                        return null;
                    }
                }
                return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                    _sqlExpressionFactory.Function(
                        sqlFunctionName,
                        arguments,
                        method.ReturnType));
            }
            if (s_spannerNumericToDecimalMethodInfo.Equals(method))
            {
                return instance;
            }
            return null;
        }
    }
}
