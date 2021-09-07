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
using Google.Cloud.Spanner.V1;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System;
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
    internal class SpannerObjectToStringTranslator : IMethodCallTranslator
    {
        private static readonly HashSet<System.Type> s_typeMapping = new HashSet<System.Type>
            {
                typeof(bool),
                typeof(byte),
                typeof(byte[]),
                typeof(char),
                typeof(DateTime),
                typeof(SpannerDate),
                typeof(decimal),
                typeof(SpannerNumeric),
                typeof(double),
                typeof(float),
                typeof(Guid),
                typeof(Regex),
                typeof(int),
                typeof(long),
                typeof(sbyte),
                typeof(short),
                typeof(ulong),
                typeof(uint),
                typeof(ushort),

                typeof(bool?),
                typeof(byte?),
                typeof(char?),
                typeof(DateTime?),
                typeof(SpannerDate?),
                typeof(decimal?),
                typeof(SpannerNumeric?),
                typeof(double?),
                typeof(float?),
                typeof(Guid?),
                typeof(int?),
                typeof(long?),
                typeof(sbyte?),
                typeof(short?),
                typeof(ulong?),
                typeof(uint?),
                typeof(ushort?),
            };

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerObjectToStringTranslator([NotNull] ISqlExpressionFactory sqlExpressionFactory)
            => _sqlExpressionFactory = sqlExpressionFactory;

        public virtual SqlExpression Translate(
            SqlExpression instance,
            MethodInfo method,
            IReadOnlyList<SqlExpression> arguments)
        {
            return method.Name == nameof(ToString)
                && arguments.Count == 0
                && instance != null
                && s_typeMapping.Contains(instance.Type)
                    ? ConvertToString(instance)
                    : null;
        }

        private SqlExpression ConvertToString(SqlExpression instance)
        {
            if (instance.Type == typeof(DateTime) || instance.Type == typeof(DateTime?))
            {
                return _sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Function(
                    "FORMAT_TIMESTAMP",
                    new[] { _sqlExpressionFactory.Constant("%FT%H:%M:%E*SZ"), instance, _sqlExpressionFactory.Constant("UTC") },
                    typeof(string)
                ));
            }
            return _sqlExpressionFactory.Convert(instance, typeof(string));
        }
    }
}
