// Copyright 2026 Google LLC
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
using System.Text.Json;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    using Type = System.Type;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    internal class SpannerJsonMethodCallTranslator : IMethodCallTranslator
    {
        // JsonElement.GetProperty method
        private static readonly MethodInfo s_getPropertyMethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetProperty), new[] { typeof(string) });

        // JsonElement.GetString method
        private static readonly MethodInfo s_getStringMethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetString), System.Array.Empty<System.Type>());

        // JsonElement.GetInt32 method
        private static readonly MethodInfo s_getInt32MethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetInt32), System.Array.Empty<System.Type>());

        // JsonElement.GetInt64 method  
        private static readonly MethodInfo s_getInt64MethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetInt64), System.Array.Empty<System.Type>());

        // JsonElement.GetBoolean method
        private static readonly MethodInfo s_getBooleanMethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetBoolean), System.Array.Empty<System.Type>());

        // JsonElement.GetDouble method
        private static readonly MethodInfo s_getDoubleMethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetDouble), System.Array.Empty<System.Type>());

        private static readonly MethodInfo s_getInt16MethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetInt16), Type.EmptyTypes);

        private static readonly MethodInfo s_getByteMethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetByte), Type.EmptyTypes);

        private static readonly MethodInfo s_getSByteMethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetSByte), Type.EmptyTypes);

        private static readonly MethodInfo s_getSingleMethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetSingle), Type.EmptyTypes);

        private static readonly MethodInfo s_getDecimalMethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetDecimal), Type.EmptyTypes);

        private static readonly MethodInfo s_getDateTimeMethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetDateTime), Type.EmptyTypes);

        private static readonly MethodInfo s_getGuidMethodInfo
            = typeof(JsonElement).GetRuntimeMethod(nameof(JsonElement.GetGuid), Type.EmptyTypes);

        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerJsonMethodCallTranslator([NotNull] ISqlExpressionFactory sqlExpressionFactory)
        {
            _sqlExpressionFactory = sqlExpressionFactory;
        }

        public virtual SqlExpression Translate(
            SqlExpression instance,
            MethodInfo method,
            IReadOnlyList<SqlExpression> arguments,
            IDiagnosticsLogger<DbLoggerCategory.Query> logger)
        {
            if (instance == null)
            {
                return null;
            }

            // Handle JsonElement.GetProperty - access a nested JSON property using JSON_QUERY.
            // Cloud Spanner requires the path argument of JSON_QUERY to be a literal or query parameter,
            // so only constant property names are translated to a combined JSONPath literal.
            if (s_getPropertyMethodInfo?.Equals(method) == true && arguments.Count == 1)
            {
                if (arguments[0] is not SqlConstantExpression { Value: string propertyName })
                {
                    return null;
                }

                var propertySegment = SpannerJsonPathHelper.FormatPropertySegmentForConstant(propertyName);
                var jsonTypeMapping = instance.TypeMapping;

                if (instance is SqlFunctionExpression
                    {
                        Name: "JSON_QUERY",
                        Arguments: [var targetExpression, SqlConstantExpression { Value: string parentPath }]
                    })
                {
                    var combinedPath = parentPath + propertySegment;
                    return _sqlExpressionFactory.Function(
                        "JSON_QUERY",
                        new[] { targetExpression, _sqlExpressionFactory.Constant(combinedPath) },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true, true },
                        typeof(JsonElement),
                        jsonTypeMapping);
                }

                var jsonPath = "$" + propertySegment;
                return _sqlExpressionFactory.Function(
                    "JSON_QUERY",
                    new[] { instance, _sqlExpressionFactory.Constant(jsonPath) },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true, true },
                    typeof(JsonElement),
                    jsonTypeMapping);
            }

            // Handle JsonElement scalar getter methods
            if (s_getStringMethodInfo?.Equals(method) == true)
            {
                return ExtractJsonValue(instance);
            }

            if (s_getInt32MethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(ExtractJsonValue(instance), typeof(int));
            }

            if (s_getInt64MethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(ExtractJsonValue(instance), typeof(long));
            }

            if (s_getInt16MethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(ExtractJsonValue(instance), typeof(short));
            }

            if (s_getByteMethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(ExtractJsonValue(instance), typeof(byte));
            }

            if (s_getSByteMethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(ExtractJsonValue(instance), typeof(sbyte));
            }

            if (s_getBooleanMethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(ExtractJsonValue(instance), typeof(bool));
            }

            if (s_getDoubleMethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(ExtractJsonValue(instance), typeof(double));
            }

            if (s_getSingleMethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(ExtractJsonValue(instance), typeof(float));
            }

            if (s_getDecimalMethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(ExtractJsonValue(instance), typeof(decimal));
            }

            if (s_getDateTimeMethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(ExtractJsonValue(instance), typeof(DateTime));
            }

            if (s_getGuidMethodInfo?.Equals(method) == true)
            {
                return ExtractJsonValue(instance, typeof(Guid));
            }

            return null;
        }

        private SqlExpression ExtractJsonValue(SqlExpression instance, Type returnType = null)
        {
            returnType ??= typeof(string);

            if (instance is SqlFunctionExpression
                {
                    Name: "JSON_QUERY",
                    Arguments: [var targetExpression, var pathExpression]
                })
            {
                return _sqlExpressionFactory.Function(
                    "JSON_VALUE",
                    new[] { targetExpression, pathExpression },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true, true },
                    returnType);
            }

            return _sqlExpressionFactory.Function(
                "JSON_VALUE",
                new[] { instance, _sqlExpressionFactory.Constant("$") },
                nullable: true,
                argumentsPropagateNullability: new[] { true, true },
                returnType);
        }
    }
}
