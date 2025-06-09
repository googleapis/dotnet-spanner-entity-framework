// Copyright 2024 Google LLC
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

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    /// <summary>
    /// This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    /// the same compatibility standards as public APIs. It may be changed or removed without notice in
    /// any release. You should only use it directly in your code with extreme caution and knowing that
    /// doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    internal class SpannerSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
    {
        private readonly ISqlExpressionFactory _sqlExpressionFactory;

        public SpannerSqlTranslatingExpressionVisitor(
            RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
            QueryCompilationContext queryCompilationContext,
            QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
            : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
        {
            _sqlExpressionFactory = dependencies.SqlExpressionFactory;
        }

        /// <summary>
        /// Generates a SQL GREATEST expression for Cloud Spanner.
        /// Cloud Spanner supports the GREATEST function with multiple arguments.
        /// </summary>
        public override SqlExpression? GenerateGreatest(IReadOnlyList<SqlExpression> expressions, System.Type resultType)
        {
            if (expressions.Count < 2)
            {
                return null;
            }

            var resultTypeMapping = Microsoft.EntityFrameworkCore.Query.ExpressionExtensions.InferTypeMapping(expressions);

            return _sqlExpressionFactory.Function(
                "GREATEST",
                expressions,
                nullable: true,
                argumentsPropagateNullability: Enumerable.Repeat(true, expressions.Count),
                resultType,
                resultTypeMapping);
        }

        /// <summary>
        /// Generates a SQL LEAST expression for Cloud Spanner.
        /// Cloud Spanner supports the LEAST function with multiple arguments.
        /// </summary>
        public override SqlExpression? GenerateLeast(IReadOnlyList<SqlExpression> expressions, System.Type resultType)
        {
            if (expressions.Count < 2)
            {
                return null;
            }

            var resultTypeMapping = Microsoft.EntityFrameworkCore.Query.ExpressionExtensions.InferTypeMapping(expressions);

            return _sqlExpressionFactory.Function(
                "LEAST",
                expressions,
                nullable: true,
                argumentsPropagateNullability: Enumerable.Repeat(true, expressions.Count),
                resultType,
                resultTypeMapping);
        }
    }
}
