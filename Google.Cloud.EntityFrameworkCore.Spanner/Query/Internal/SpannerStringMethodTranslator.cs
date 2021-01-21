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
    public class SpannerStringMethodTranslator : IMethodCallTranslator
    {
        private static readonly MethodInfo _containsMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.Contains), new[] { typeof(string) });

        private static readonly MethodInfo _startsWithMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.StartsWith), new[] { typeof(string) });

        private static readonly MethodInfo _endsWithMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.EndsWith), new[] { typeof(string) });

        private static readonly MethodInfo _indexOfMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.IndexOf), new[] { typeof(string) });

        private static readonly MethodInfo _replaceMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.Replace), new[] { typeof(string), typeof(string) });

        private static readonly MethodInfo _toLowerMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.ToLower), Array.Empty<System.Type>());

        private static readonly MethodInfo _toUpperMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.ToUpper), Array.Empty<System.Type>());

        private static readonly MethodInfo _substringMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.Substring), new[] { typeof(int) });

        private static readonly MethodInfo _substringWithLengthMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.Substring), new[] { typeof(int), typeof(int) });

        private static readonly MethodInfo _trimStartMethodInfoWithoutArgs
            = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), Array.Empty<System.Type>());

        private static readonly MethodInfo _trimStartMethodInfoWithCharArg
            = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), new[] { typeof(char) });

        private static readonly MethodInfo _trimEndMethodInfoWithoutArgs
            = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), Array.Empty<System.Type>());

        private static readonly MethodInfo _trimEndMethodInfoWithCharArg
            = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), new[] { typeof(char) });

        private static readonly MethodInfo _trimMethodInfoWithoutArgs
            = typeof(string).GetRuntimeMethod(nameof(string.Trim), Array.Empty<System.Type>());

        private static readonly MethodInfo _trimMethodInfoWithCharArg
            = typeof(string).GetRuntimeMethod(nameof(string.Trim), new[] { typeof(char) });

        private static readonly MethodInfo _padLeftMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.PadLeft), new[] { typeof(int) });

        private static readonly MethodInfo _padLeftWithStringMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.PadLeft), new[] { typeof(int), typeof(char) });

        private static readonly MethodInfo _padRightMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.PadRight), new[] { typeof(int) });

        private static readonly MethodInfo _padRightWithStringMethodInfo
            = typeof(string).GetRuntimeMethod(nameof(string.PadRight), new[] { typeof(int), typeof(char) });

        private static readonly MethodInfo _formatOneArgMethodInfo
            = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object) });

        private static readonly MethodInfo _formatTwoArgMethodInfo
            = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object), typeof(object) });

        private static readonly MethodInfo _formatThreeArgMethodInfo
            = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object), typeof(object), typeof(object) });

        // TODO(loite): This one is never picked up by EF Core.
        private static readonly MethodInfo _formatVarArgMethodInfo
            = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object[]) });

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerStringMethodTranslator([NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public virtual SqlExpression Translate(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
        {
            if (_containsMethodInfo.Equals(method))
            {
                var pos = TranslateOneArgFunction("STRPOS", instance, arguments[0], typeof(long));
                return _sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.GreaterThan(pos, _sqlExpressionFactory.Constant(0L)));
            }
            if (_startsWithMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("STARTS_WITH", instance, arguments[0], typeof(bool));
            }
            if (_endsWithMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("ENDS_WITH", instance, arguments[0], typeof(bool));
            }
            if (_indexOfMethodInfo.Equals(method))
            {
                var pos = TranslateOneArgFunction("STRPOS", instance, arguments[0], typeof(long));
                return _sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Subtract(pos, _sqlExpressionFactory.Constant(1L)));
            }
            if (_replaceMethodInfo.Equals(method))
            {
                return TranslateTwoArgFunction("REPLACE", instance, arguments[0], arguments[1], typeof(string));
            }
            if (_toLowerMethodInfo.Equals(method))
            {
                return TranslateNoArgFunction("LOWER", instance, typeof(string));
            }
            if (_toUpperMethodInfo.Equals(method))
            {
                return TranslateNoArgFunction("UPPER", instance, typeof(string));
            }
            if (_substringMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("SUBSTR", instance, _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1L)), typeof(string));
            }
            if (_substringWithLengthMethodInfo.Equals(method))
            {
                return TranslateTwoArgFunction("SUBSTR", instance, _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1L)), arguments[1], typeof(string));
            }
            if (_trimStartMethodInfoWithoutArgs.Equals(method))
            {
                return TranslateNoArgFunction("LTRIM", instance, typeof(string));
            }
            if (_trimStartMethodInfoWithCharArg.Equals(method))
            {
                return TranslateOneArgFunction("LTRIM", instance, arguments[0], typeof(string));
            }
            if (_trimEndMethodInfoWithoutArgs.Equals(method))
            {
                return TranslateNoArgFunction("RTRIM", instance, typeof(string));
            }
            if (_trimEndMethodInfoWithCharArg.Equals(method))
            {
                return TranslateOneArgFunction("RTRIM", instance, arguments[0], typeof(string));
            }
            if (_trimMethodInfoWithoutArgs.Equals(method))
            {
                return TranslateNoArgFunction("TRIM", instance, typeof(string));
            }
            if (_trimMethodInfoWithCharArg.Equals(method))
            {
                return TranslateOneArgFunction("TRIM", instance, arguments[0], typeof(string));
            }
            if (_padLeftMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("LPAD", instance, arguments[0], typeof(string));
            }
            if (_padLeftWithStringMethodInfo.Equals(method))
            {
                return TranslateTwoArgFunction("LPAD", instance, arguments[0], arguments[1], typeof(string));
            }
            if (_padRightMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("RPAD", instance, arguments[0], typeof(string));
            }
            if (_padRightWithStringMethodInfo.Equals(method))
            {
                return TranslateTwoArgFunction("RPAD", instance, arguments[0], arguments[1], typeof(string));
            }
            if (_formatOneArgMethodInfo.Equals(method) || _formatTwoArgMethodInfo.Equals(method) || _formatThreeArgMethodInfo.Equals(method) || _formatVarArgMethodInfo.Equals(method))
            {
                return TranslateStaticFunction("FORMAT", arguments, typeof(string));
            }

            return null;
        }

        private SqlExpression TranslateStaticFunction(string function, IReadOnlyList<SqlExpression> arguments, System.Type returnType)
        {
            return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                _sqlExpressionFactory.Function(
                function,
                arguments,
                returnType));
        }

        private SqlExpression TranslateNoArgFunction(string function, SqlExpression instance, System.Type returnType)
        {
            return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                _sqlExpressionFactory.Function(
                function,
                new[] { instance },
                returnType));
        }

        private SqlExpression TranslateOneArgFunction(string function, SqlExpression instance, SqlExpression arg, System.Type returnType)
        {
            return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                _sqlExpressionFactory.Function(
                function,
                new[] { instance, arg },
                returnType));
        }

        private SqlExpression TranslateTwoArgFunction(string function, SqlExpression instance, SqlExpression arg1, SqlExpression arg2, System.Type returnType)
        {
            return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                _sqlExpressionFactory.Function(
                function,
                new[] { instance, arg1, arg2 },
                returnType));
        }
    }
}
