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

            // Handle JsonElement.GetProperty - this accesses a nested JSON property
            // For example: jsonColumn.GetProperty("propertyName")
            // Note: This is typically handled by EF Core's JsonScalarExpression generation
            // and processed by VisitJsonScalar. We return null here to let EF Core's
            // default translation handle this case.
            if (s_getPropertyMethodInfo?.Equals(method) == true && arguments.Count == 1)
            {
                // Return null to indicate we don't handle this - EF Core will use JsonScalarExpression
                return null;
            }

            // Handle JsonElement.GetString - extract string value from JSON
            if (s_getStringMethodInfo?.Equals(method) == true)
            {
                // Cast the JSON value to STRING type
                // Spanner will automatically extract the scalar value when casting
                return _sqlExpressionFactory.Convert(instance, typeof(string));
            }

            // Handle JsonElement.GetInt32 - extract int value from JSON
            if (s_getInt32MethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(instance, typeof(int));
            }

            // Handle JsonElement.GetInt64 - extract long value from JSON
            if (s_getInt64MethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(instance, typeof(long));
            }

            // Handle JsonElement.GetBoolean - extract bool value from JSON
            if (s_getBooleanMethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(instance, typeof(bool));
            }

            // Handle JsonElement.GetDouble - extract double value from JSON
            if (s_getDoubleMethodInfo?.Equals(method) == true)
            {
                return _sqlExpressionFactory.Convert(instance, typeof(double));
            }

            return null;
        }
    }
}
