// Copyright 2022 Google LLC
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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal;

/// <summary>
///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
///     the same compatibility standards as public APIs. It may be changed or removed without notice in
///     any release. You should only use it directly in your code with extreme caution and knowing that
///     doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
internal class SpannerListMethodCallTranslator : IMethodCallTranslator
{
    private readonly SpannerSqlExpressionFactory _sqlExpressionFactory;

    private static readonly MethodInfo s_enumerableContains =
        typeof(Enumerable).GetTypeInfo().GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Single(m => m.Name == nameof(Enumerable.Contains) && m.GetParameters().Length == 2);

    public SpannerListMethodCallTranslator([NotNull] SpannerSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression Translate(SqlExpression instance, MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.IsClosedFormOf(s_enumerableContains)
            && arguments[0].Type.IsArrayOrGenericList()
            // Handle either array columns (with an array mapping) or parameters/constants (no mapping)
            && arguments[0].TypeMapping is SpannerDateArrayTypeMapping 
                or SpannerJsonArrayTypeMapping 
                or SpannerComplexTypeMapping { IsArrayType: true } 
                or null
            )
        {
            var valuesExpression =
                _sqlExpressionFactory.Function("UNNEST", new[] { arguments[0] }, false,
                    new[] { true }, arguments[0].Type);
            return _sqlExpressionFactory.SpannerContains(arguments[1], valuesExpression, false);
        }
        
        return null;
    }
}
