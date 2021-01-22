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

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public class SpannerNullableMethodTranslator : IMethodCallTranslator
    {
        private static readonly Dictionary<MethodInfo, object> _defaultMethods = new Dictionary<MethodInfo, object>
        {
            { typeof(bool?).GetRuntimeMethod(nameof(Nullable<bool>.GetValueOrDefault), Array.Empty<System.Type>()), false },
            { typeof(byte?).GetRuntimeMethod(nameof(Nullable<byte>.GetValueOrDefault), Array.Empty<System.Type>()), (byte) 0 },
            { typeof(sbyte?).GetRuntimeMethod(nameof(Nullable<sbyte>.GetValueOrDefault), Array.Empty<System.Type>()), (sbyte) 0 },
            { typeof(short?).GetRuntimeMethod(nameof(Nullable<short>.GetValueOrDefault), Array.Empty<System.Type>()), (short) 0 },
            { typeof(int?).GetRuntimeMethod(nameof(Nullable<int>.GetValueOrDefault), Array.Empty<System.Type>()), 0 },
            { typeof(uint?).GetRuntimeMethod(nameof(Nullable<uint>.GetValueOrDefault), Array.Empty<System.Type>()), 0 },
            { typeof(long?).GetRuntimeMethod(nameof(Nullable<long>.GetValueOrDefault), Array.Empty<System.Type>()), 0L },
            { typeof(ulong?).GetRuntimeMethod(nameof(Nullable<ulong>.GetValueOrDefault), Array.Empty<System.Type>()), 0L },
            { typeof(float?).GetRuntimeMethod(nameof(Nullable<float>.GetValueOrDefault), Array.Empty<System.Type>()), 0f },
            { typeof(double?).GetRuntimeMethod(nameof(Nullable<double>.GetValueOrDefault), Array.Empty<System.Type>()), 0d },
            { typeof(SpannerNumeric?).GetRuntimeMethod(nameof(Nullable<SpannerNumeric>.GetValueOrDefault), Array.Empty<System.Type>()), SpannerNumeric.Zero },
            { typeof(SpannerDate?).GetRuntimeMethod(nameof(Nullable<SpannerDate>.GetValueOrDefault), Array.Empty<System.Type>()), new SpannerDate(1, 1, 1) },
            { typeof(DateTime?).GetRuntimeMethod(nameof(Nullable<DateTime>.GetValueOrDefault), Array.Empty<System.Type>()), new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
        };
        private static readonly HashSet<MethodInfo> _defaultMethodsWithArgument = new HashSet<MethodInfo>
        {
            typeof(bool?).GetRuntimeMethod(nameof(Nullable<bool>.GetValueOrDefault), new[] { typeof(bool) }),
            typeof(byte?).GetRuntimeMethod(nameof(Nullable<byte>.GetValueOrDefault), new[] { typeof(byte) }),
            typeof(sbyte?).GetRuntimeMethod(nameof(Nullable<sbyte>.GetValueOrDefault), new[] { typeof(sbyte) }),
            typeof(short?).GetRuntimeMethod(nameof(Nullable<short>.GetValueOrDefault), new[] { typeof(short) }),
            typeof(int?).GetRuntimeMethod(nameof(Nullable<int>.GetValueOrDefault), new[] { typeof(int) }),
            typeof(uint?).GetRuntimeMethod(nameof(Nullable<uint>.GetValueOrDefault), new[] { typeof(uint) }),
            typeof(long?).GetRuntimeMethod(nameof(Nullable<long>.GetValueOrDefault), new[] { typeof(long) }),
            typeof(ulong?).GetRuntimeMethod(nameof(Nullable<ulong>.GetValueOrDefault), new[] { typeof(ulong) }),
            typeof(float?).GetRuntimeMethod(nameof(Nullable<float>.GetValueOrDefault), new[] { typeof(float) }),
            typeof(double?).GetRuntimeMethod(nameof(Nullable<double>.GetValueOrDefault), new[] { typeof(double) }),
            typeof(SpannerNumeric?).GetRuntimeMethod(nameof(Nullable<SpannerNumeric>.GetValueOrDefault), new[] { typeof(SpannerNumeric) }),
            typeof(SpannerDate?).GetRuntimeMethod(nameof(Nullable<SpannerDate>.GetValueOrDefault), new[] { typeof(SpannerDate) }),
            typeof(DateTime?).GetRuntimeMethod(nameof(Nullable<DateTime>.GetValueOrDefault), new[] { typeof(DateTime) }),
        };

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerNullableMethodTranslator([NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public virtual SqlExpression Translate(SqlExpression instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
        {
            if (_defaultMethods.TryGetValue(method, out var defaultValue))
            {
                return _sqlExpressionFactory.Coalesce(instance, _sqlExpressionFactory.Constant(defaultValue), instance.TypeMapping);
            }
            if (_defaultMethodsWithArgument.Contains(method))
            {
                return _sqlExpressionFactory.Coalesce(instance, arguments[0], instance.TypeMapping);
            }
            return null;
        }
    }
}
