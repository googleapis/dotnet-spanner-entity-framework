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
using System.Reflection;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerStringMemberTranslator : IMemberTranslator
    {
        private static readonly PropertyInfo _lengthMethodInfo
            = typeof(string).GetRuntimeProperty(nameof(string.Length));

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerStringMemberTranslator([NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public SqlExpression Translate(SqlExpression instance, MemberInfo member, System.Type returnType)
        {
            if (_lengthMethodInfo.Equals(member))
            {
                return TranslateNoArgFunction("CHAR_LENGTH", instance, typeof(int));
            }

            return null;
        }

        private SqlExpression TranslateNoArgFunction(string function, SqlExpression instance, System.Type returnType)
        {
            return _sqlExpressionFactory.ApplyDefaultTypeMapping(
                _sqlExpressionFactory.Function(
                function,
                new[] { instance },
                returnType));
        }
    }
}
