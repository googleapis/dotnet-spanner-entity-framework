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
using System.Collections.Generic;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerRegexMethodTranslator : IMethodCallTranslator
    {
        private static readonly MethodInfo _isMatchMethodInfo
            = typeof(Regex).GetRuntimeMethod(nameof(Regex.IsMatch), new[] { typeof(string) });

        private static readonly MethodInfo _isMatchStaticMethodInfo
            = typeof(Regex).GetRuntimeMethod(nameof(Regex.IsMatch), new[] { typeof(string), typeof(string) });

        private static readonly MethodInfo _replaceMethodInfo
            = typeof(Regex).GetRuntimeMethod(nameof(Regex.Replace), new[] { typeof(string), typeof(string) });

        private static readonly MethodInfo _replaceStaticMethodInfo
            = typeof(Regex).GetRuntimeMethod(nameof(Regex.Replace), new[] { typeof(string), typeof(string), typeof(string) });

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerRegexMethodTranslator([NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public virtual SqlExpression Translate(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
        {
            if (_isMatchMethodInfo.Equals(method))
            {
                // Yes, this is the correct order. Cloud Spanner expects the input string as the first parameter, and then the pattern.
                return TranslateOneArgFunction("REGEXP_CONTAINS", arguments[0], AddBeginAndEndOfText(instance), typeof(bool));
            }
            if (_isMatchStaticMethodInfo.Equals(method))
            {
                return TranslateOneArgFunction("REGEXP_CONTAINS", AddBeginAndEndOfText(arguments[0]), arguments[1], typeof(bool));
            }
            if (_replaceMethodInfo.Equals(method))
            {
                // Yes, this is the correct order. Cloud Spanner expects the input string as the first parameter, and then the pattern.
                return TranslateTwoArgFunction("REGEXP_REPLACE", arguments[0], instance, arguments[1], typeof(string));
            }
            if (_replaceStaticMethodInfo.Equals(method))
            {
                return TranslateStaticFunction("REGEXP_REPLACE", arguments, typeof(string));
            }

            return null;
        }

        private SqlExpression AddBeginAndEndOfText(SqlExpression expression)
        {
            var prepended = _sqlExpressionFactory.Add(_sqlExpressionFactory.Constant("^"), expression);
            var appended = _sqlExpressionFactory.Add(prepended, _sqlExpressionFactory.Constant("$"));
            return appended;
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
