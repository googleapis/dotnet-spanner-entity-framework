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
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    internal class SpannerStringMethodTranslator : IMethodCallTranslator
    {
        private static readonly MethodInfo s_containsMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.Contains), new[] { typeof(string) });

        private static readonly MethodInfo s_startsWithMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.StartsWith), new[] { typeof(string) });

        private static readonly MethodInfo s_endsWithMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.EndsWith), new[] { typeof(string) });

        private static readonly MethodInfo s_indexOfMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.IndexOf), new[] { typeof(string) });

        private static readonly MethodInfo s_replaceMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.Replace), new[] { typeof(string), typeof(string) });

        private static readonly MethodInfo s_toLowerMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.ToLower), Array.Empty<System.Type>());

        private static readonly MethodInfo s_toUpperMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.ToUpper), Array.Empty<System.Type>());

        private static readonly MethodInfo s_substringMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.Substring), new[] { typeof(int) });

        private static readonly MethodInfo s_substringWithLengthMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.Substring), new[] { typeof(int), typeof(int) });

        private static readonly MethodInfo s_trimStartMethodInfoWithoutArgs
            = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), Array.Empty<System.Type>());

        private static readonly MethodInfo s_trimStartMethodInfoWithCharArg
            = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), new[] { typeof(char) });

        private static readonly MethodInfo s_trimEndMethodInfoWithoutArgs
            = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), Array.Empty<System.Type>());

        private static readonly MethodInfo s_trimEndMethodInfoWithCharArg
            = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), new[] { typeof(char) });

        private static readonly MethodInfo s_trimMethodInfoWithoutArgs
            = typeof(string).GetRuntimeMethod(nameof(string.Trim), Array.Empty<System.Type>());

        private static readonly MethodInfo s_trimMethodInfoWithCharArg
            = typeof(string).GetRuntimeMethod(nameof(string.Trim), new[] { typeof(char) });

        private static readonly MethodInfo s_padLeftMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.PadLeft), new[] { typeof(int) });

        private static readonly MethodInfo s_padLeftWithStringMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.PadLeft), new[] { typeof(int), typeof(char) });

        private static readonly MethodInfo s_padRightMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.PadRight), new[] { typeof(int) });

        private static readonly MethodInfo s_padRightWithStringMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.PadRight), new[] { typeof(int), typeof(char) });

        private static readonly MethodInfo s_formatOneArgMethodInfo
            = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object) });

        private static readonly MethodInfo s_formatTwoArgMethodInfo
            = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object), typeof(object) });

        private static readonly MethodInfo s_formatThreeArgMethodInfo
            = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object), typeof(object), typeof(object) });

        // TODO(loite): This one is never picked up by EF Core.
        private static readonly MethodInfo s_formatVarArgMethodInfo
            = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object[]) });

        private static readonly MethodInfo s_joinMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.Join), new[] { typeof(string), typeof(IEnumerable<string>) });
        
        private static readonly MethodInfo s_concatMethodInfoTwoArgs
            = typeof(string).GetRuntimeMethod(nameof(string.Concat), new[] { typeof(string), typeof(string) })!;

        private static readonly MethodInfo s_concatMethodInfoThreeArgs
            = typeof(string).GetRuntimeMethod(nameof(string.Concat), new[] { typeof(string), typeof(string), typeof(string) })!;

        private static readonly MethodInfo s_concatMethodInfoFourArgs
            = typeof(string).GetRuntimeMethod(
                nameof(string.Concat), new[] { typeof(string), typeof(string), typeof(string), typeof(string) })!;
        
        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerStringMethodTranslator([NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public virtual SqlExpression Translate(
            SqlExpression instance,
            MethodInfo method,
            IReadOnlyList<SqlExpression> arguments,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            if (s_containsMethodInfo.Equals(method))
            {
                var pos = TranslateOneArgFunction("STRPOS", instance, arguments[0], typeof(long));
                return _sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.GreaterThan(pos, _sqlExpressionFactory.Constant(0L)));
            }
            if (s_startsWithMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("STARTS_WITH", instance, arguments[0], typeof(bool));
            }
            if (s_endsWithMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("ENDS_WITH", instance, arguments[0], typeof(bool));
            }
            if (s_indexOfMethodInfo.Equals(method))
            {
                var pos = TranslateOneArgFunction("STRPOS", instance, arguments[0], typeof(long));
                return _sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Subtract(pos, _sqlExpressionFactory.Constant(1L)));
            }
            if (s_replaceMethodInfo.Equals(method))
            {
                return TranslateTwoArgFunction("REPLACE", instance, arguments[0], arguments[1], typeof(string));
            }
            if (s_toLowerMethodInfo.Equals(method))
            {
                return TranslateNoArgFunction("LOWER", instance, typeof(string));
            }
            if (s_toUpperMethodInfo.Equals(method))
            {
                return TranslateNoArgFunction("UPPER", instance, typeof(string));
            }
            if (s_substringMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("SUBSTR", instance, _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1L)), typeof(string));
            }
            if (s_substringWithLengthMethodInfo.Equals(method))
            {
                return TranslateTwoArgFunction("SUBSTR", instance, _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1L)), arguments[1], typeof(string));
            }
            if (s_trimStartMethodInfoWithoutArgs.Equals(method))
            {
                return TranslateNoArgFunction("LTRIM", instance, typeof(string));
            }
            if (s_trimStartMethodInfoWithCharArg.Equals(method))
            {
                return TranslateOneArgFunction("LTRIM", instance, arguments[0], typeof(string));
            }
            if (s_trimEndMethodInfoWithoutArgs.Equals(method))
            {
                return TranslateNoArgFunction("RTRIM", instance, typeof(string));
            }
            if (s_trimEndMethodInfoWithCharArg.Equals(method))
            {
                return TranslateOneArgFunction("RTRIM", instance, arguments[0], typeof(string));
            }
            if (s_trimMethodInfoWithoutArgs.Equals(method))
            {
                return TranslateNoArgFunction("TRIM", instance, typeof(string));
            }
            if (s_trimMethodInfoWithCharArg.Equals(method))
            {
                return TranslateOneArgFunction("TRIM", instance, arguments[0], typeof(string));
            }
            if (s_padLeftMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("LPAD", instance, arguments[0], typeof(string));
            }
            if (s_padLeftWithStringMethodInfo.Equals(method))
            {
                return TranslateTwoArgFunction("LPAD", instance, arguments[0], arguments[1], typeof(string));
            }
            if (s_padRightMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("RPAD", instance, arguments[0], typeof(string));
            }
            if (s_padRightWithStringMethodInfo.Equals(method))
            {
                return TranslateTwoArgFunction("RPAD", instance, arguments[0], arguments[1], typeof(string));
            }
            if (s_formatOneArgMethodInfo.Equals(method) || s_formatTwoArgMethodInfo.Equals(method) || s_formatThreeArgMethodInfo.Equals(method) || s_formatVarArgMethodInfo.Equals(method))
            {
                return TranslateStaticFunction("FORMAT", arguments, typeof(string));
            }
            if (s_joinMethodInfo.Equals(method))
            {
                return TranslateTwoArgFunction("ARRAY_TO_STRING", arguments[1], arguments[0], _sqlExpressionFactory.Constant(""), typeof(string));
            }
            if (s_concatMethodInfoTwoArgs.Equals(method))
            {
                return TranslateStaticFunction("CONCAT", arguments, typeof(string));
            }
            if (s_concatMethodInfoThreeArgs.Equals(method))
            {
                return TranslateStaticFunction("CONCAT", arguments, typeof(string));
            }
            if (s_concatMethodInfoFourArgs.Equals(method))
            {
                return TranslateStaticFunction("CONCAT", arguments, typeof(string));
            }

            return null;
        }

        private SqlExpression TranslateStaticFunction(string function, IReadOnlyList<SqlExpression> arguments, System.Type returnType)
        {
            var nullabilityPropagation = new bool[arguments.Count];
            Array.Fill(nullabilityPropagation, true);
            return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                _sqlExpressionFactory.Function(
                function,
                arguments,
                true,
                nullabilityPropagation,
                returnType));
        }

        private SqlExpression TranslateNoArgFunction(string function, SqlExpression instance, System.Type returnType)
        {
            return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                _sqlExpressionFactory.Function(
                function,
                new[] { instance },
                true,
                new []{true},
                returnType));
        }

        private SqlExpression TranslateOneArgFunction(string function, SqlExpression instance, SqlExpression arg, System.Type returnType)
        {
            return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                _sqlExpressionFactory.Function(
                function,
                new[] { instance, arg },
                true,
                new []{true, true},
                returnType));
        }

        private SqlExpression TranslateTwoArgFunction(string function, SqlExpression instance, SqlExpression arg1, SqlExpression arg2, System.Type returnType)
        {
            return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                _sqlExpressionFactory.Function(
                function,
                new[] { instance, arg1, arg2 },
                true,
                new []{true, true, true},
                returnType));
        }
    }
}
