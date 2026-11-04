// Copyright 2020, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Api.Gax;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using System;
using System.Collections;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    internal class SpannerQuerySqlGenerator : QuerySqlGenerator
    {
        public SpannerQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
            : base(dependencies)
        {
        }

        protected override Expression VisitExtension(Expression extensionExpression)
        {
            return extensionExpression switch
            {
                SpannerContainsExpression containsExpression => VisitContains(containsExpression),
                _ => base.VisitExtension(extensionExpression),
            };
        }

        protected override void GenerateTop(SelectExpression selectExpression)
        {
        }

        protected override string GetOperator(SqlBinaryExpression binaryExpression)
        {
            if (binaryExpression.OperatorType == ExpressionType.Add)
            {
                if ((binaryExpression.Left.TypeMapping.StoreTypeNameBase == "STRING" || binaryExpression.Left.TypeMapping.StoreTypeNameBase == "BYTES")
                    && (binaryExpression.Right.TypeMapping.StoreTypeNameBase == "STRING" || binaryExpression.Right.TypeMapping.StoreTypeNameBase == "BYTES"))
                {
                    return "||";
                }
            }
            return base.GetOperator(binaryExpression);
        }

        protected override void GenerateLimitOffset(SelectExpression selectExpression)
        {
            GaxPreconditions.CheckNotNull(selectExpression, nameof(selectExpression));

            if (selectExpression.Limit != null)
            {
                Sql.AppendLine().Append("LIMIT ");
                Visit(selectExpression.Limit);
            }
            else if (selectExpression.Offset != null)
            {
                // Cloud Spanner requires limit if offset is specified.
                // So we create a LIMIT clause that contains the maximum possible number of rows,
                // which means INT64.MAX_VALUE - OFFSET.
                long limit;
                if (selectExpression.Offset is SqlConstantExpression sqlConstantExpression)
                {
                    limit = long.MaxValue - Convert.ToInt64(sqlConstantExpression.Value);
                }
                else
                {
                    // We can't get the value here, so we need to just set a very high value.
                    limit = long.MaxValue / 2;
                }
                Sql.AppendLine().Append($"LIMIT {limit}");
            }

            if (selectExpression.Offset == null)
            {
                return;
            }
            Sql.Append(" OFFSET ");
            Visit(selectExpression.Offset);
        }

        protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
        {
            switch (sqlFunctionExpression.Name)
            {
                case "CAST":
                    {
                        Sql.Append(sqlFunctionExpression.Name);
                        Sql.Append("(");

                        Visit(sqlFunctionExpression.Arguments[0]);

                        Sql.Append(" AS ");

                        Visit(sqlFunctionExpression.Arguments[1]);

                        Sql.Append(")");

                        return sqlFunctionExpression;
                    }
                case "LN":
                case "LOG":
                case "LOG10":
                    //Since spanner does not attempt any short circuit eval,
                    //we make these methods return NaN instead of throw
                    //Otherwise, where clauses such as WHERE x > 0 AND LN(x) < [foo]
                    //will throws because the protection of "x > 0" does not stop LN(0)
                    //from being evaluated.
                    Sql.Append("IF(");
                    Visit(sqlFunctionExpression.Arguments[0]);
                    Sql.Append("<=0, CAST('NaN' AS FLOAT64), ");

                    base.VisitSqlFunction(sqlFunctionExpression);

                    Sql.Append(")");
                    return sqlFunctionExpression;
            }

            return base.VisitSqlFunction(sqlFunctionExpression);
        }
        
        protected override void GenerateIn(InExpression inExpression, bool negated)
        {
            if (inExpression is not SpannerInExpression)
            {
                base.GenerateIn(inExpression, negated);
                return;
            }
            Visit(inExpression.Item);
            Sql.Append(negated ? " NOT IN " : " IN ");
            Sql.Append(" UNNEST (");
            Visit(inExpression.ValuesParameter);
            Sql.Append(")");
        }

        protected virtual Expression VisitContains(SpannerContainsExpression containsExpression)
        {
            var valueRequiresParentheses = ValueRequiresParentheses(containsExpression.Values);
            Visit(containsExpression.Item);
            Sql.Append(containsExpression.IsNegated ? " NOT IN " : " IN ");

            if (valueRequiresParentheses)
            {
                Sql.Append("(");
            }

            if (containsExpression.Values is SqlConstantExpression constantValuesExpression
                && constantValuesExpression.Value is IEnumerable constantValues)
            {
                var first = true;
                foreach (var item in constantValues)
                {
                    if (!first)
                    {
                        Sql.Append(", ");
                    }

                    first = false;
                    Sql.Append(constantValuesExpression.TypeMapping?.GenerateSqlLiteral(item) ??
                               item?.ToString() ?? "NULL");
                }
            }
            else
            {
                Visit(containsExpression.Values);
            }

            if (valueRequiresParentheses)
            {
                Sql.Append(")");
            }

            return containsExpression;

            static bool ValueRequiresParentheses(SqlExpression valueExpression)
            {
                return valueExpression switch
                {
                    SqlFunctionExpression => false,
                    _ => true,
                };
            }
        }

        /// <summary>
        /// Translates a <see cref="JsonScalarExpression"/> representing access to a scalar property
        /// within a JSON column into Cloud Spanner SQL.
        /// Cloud Spanner provides the JSON_VALUE(json, path) function to extract scalar values.
        /// Since JSON_VALUE always returns a STRING, non-string and non-JSON scalar types (such as INT64,
        /// BOOL, FLOAT64, NUMERIC, TIMESTAMP, DATE) must be wrapped in a CAST(JSON_VALUE(...) AS &lt;STORE_TYPE&gt;).
        /// String types (including sized strings, char, and Guid) and JSON types do not require a cast.
        /// </summary>
        protected override Expression VisitJsonScalar(JsonScalarExpression jsonScalarExpression)
        {
            var path = jsonScalarExpression.Path;
            if (path.Count == 0)
            {
                Visit(jsonScalarExpression.Json);
                return jsonScalarExpression;
            }

            var typeMapping = jsonScalarExpression.TypeMapping;
            var needsCast = RequiresCastFromJsonValue(typeMapping);

            if (needsCast)
            {
                Sql.Append("CAST(");
            }

            var jsonPath = SpannerJsonPathHelper.BuildJsonPath(path);
            GenerateJsonValue(jsonScalarExpression.Json, jsonPath);

            if (needsCast)
            {
                Sql.Append(" AS ");
                Sql.Append(typeMapping!.StoreTypeNameBase!.ToUpperInvariant());
                Sql.Append(")");
            }

            return jsonScalarExpression;
        }

        /// <summary>
        /// Determines whether a CAST wrapper is needed around JSON_VALUE for the specified type mapping.
        /// JSON_VALUE always returns a STRING scalar (or NULL). For STRING and JSON store types, no cast
        /// is required. Sized string mappings (e.g. STRING(36) for Guid, STRING(1) for char, or STRING(50))
        /// share the base store type "STRING"; casting to a sized string (e.g. CAST(... AS STRING(36))) is
        /// a syntax error in Cloud Spanner SQL. All other store types (e.g. INT64, BOOL, FLOAT64) must be cast.
        /// </summary>
        private static bool RequiresCastFromJsonValue(RelationalTypeMapping typeMapping)
        {
            var storeTypeNameBase = typeMapping?.StoreTypeNameBase?.ToUpperInvariant();
            return storeTypeNameBase is not (null or "STRING" or "JSON");
        }

        /// <summary>
        /// Emits the JSON_VALUE(...) SQL expression.
        /// Emits: JSON_VALUE(json, '$.path')
        /// </summary>
        private void GenerateJsonValue(SqlExpression jsonExpression, string jsonPath)
        {
            Sql.Append("JSON_VALUE(");
            Visit(jsonExpression);
            Sql.Append(", '");
            Sql.Append(jsonPath);
            Sql.Append("')");
        }
        
    }
}
