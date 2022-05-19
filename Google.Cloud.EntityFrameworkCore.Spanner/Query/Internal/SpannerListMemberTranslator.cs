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
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System;
using System.Collections.Generic;
using System.Reflection;
using SpannerDate = Google.Cloud.EntityFrameworkCore.Spanner.Storage.SpannerDate;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    internal class SpannerListMemberTranslator : IMemberTranslator
    {
        private static readonly HashSet<MemberInfo> s_countMethods = new HashSet<MemberInfo>
        {
            typeof(List<decimal>).GetRuntimeProperty(nameof(List<decimal>.Count)),
            typeof(List<decimal?>).GetRuntimeProperty(nameof(List<decimal?>.Count)),
            typeof(List<SpannerNumeric>).GetRuntimeProperty(nameof(List<SpannerNumeric>.Count)),
            typeof(List<SpannerNumeric?>).GetRuntimeProperty(nameof(List<SpannerNumeric?>.Count)),
            typeof(List<bool>).GetRuntimeProperty(nameof(List<bool>.Count)),
            typeof(List<bool?>).GetRuntimeProperty(nameof(List<bool?>.Count)),
            typeof(List<double>).GetRuntimeProperty(nameof(List<double>.Count)),
            typeof(List<double?>).GetRuntimeProperty(nameof(List<double?>.Count)),
            typeof(List<long>).GetRuntimeProperty(nameof(List<long>.Count)),
            typeof(List<long?>).GetRuntimeProperty(nameof(List<long?>.Count)),
            typeof(List<SpannerDate>).GetRuntimeProperty(nameof(List<SpannerDate>.Count)),
            typeof(List<SpannerDate?>).GetRuntimeProperty(nameof(List<SpannerDate?>.Count)),
            typeof(List<DateTime>).GetRuntimeProperty(nameof(List<DateTime>.Count)),
            typeof(List<DateTime?>).GetRuntimeProperty(nameof(List<DateTime?>.Count)),
            typeof(List<byte[]>).GetRuntimeProperty(nameof(List<byte[]>.Count)),
            typeof(List<byte?[]>).GetRuntimeProperty(nameof(List<byte?[]>.Count)),
            typeof(List<string>).GetRuntimeProperty(nameof(List<string>.Count))
        };

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerListMemberTranslator([NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public SqlExpression Translate(
            SqlExpression instance,
            MemberInfo member,
            System.Type returnType,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            if (s_countMethods.Contains(member))
            {
                return TranslateNoArgFunction("ARRAY_LENGTH", instance, typeof(int));
            }

            return null;
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
    }
}
