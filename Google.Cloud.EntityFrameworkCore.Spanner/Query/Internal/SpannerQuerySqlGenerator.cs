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
            if (inExpression.GetType() != typeof(SpannerInExpression))
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
        
        protected override Expression VisitJsonScalar(JsonScalarExpression jsonScalarExpression)
        {
            var path = jsonScalarExpression.Path;
            if (path.Count == 0)
            {
                Visit(jsonScalarExpression.Json);
                return jsonScalarExpression;
            }
            
            // JSON_VALUE in Spanner always returns STRING.
            // For non-string types, we need to wrap the result in CAST() to convert to the expected type.
            // This is similar to how SQL Server handles JSON_VALUE (which returns nvarchar(4000)).
            var typeMapping = jsonScalarExpression.TypeMapping;
            var needsCast = typeMapping != null 
                && typeMapping.StoreType != "STRING" 
                && typeMapping.StoreType != "JSON";
            
            if (needsCast)
            {
                Sql.Append("CAST(");
            }
            
            // Use JSON_VALUE with JSONPath syntax for Spanner
            // Example: JSON_VALUE(column, '$.property.nested')
            
            // Build JSONPath expression: $.property.nested or $.property[0]
            var jsonPathBuilder = new System.Text.StringBuilder("$");
            foreach (var pathSegment in path)
            {
                if (pathSegment.PropertyName != null)
                {
                    // Use dot notation for properties: $.property
                    // Escape property names if they contain special characters
                    var propertyName = pathSegment.PropertyName;
                    
                    // If property name has special characters, use double-quoted notation in JSONPath
                    // Example: $."property.with.dot" instead of $["property.with.dot"]
                    if (propertyName.Contains(".") || propertyName.Contains("'") || propertyName.Contains("\"") || propertyName.Contains(" "))
                    {
                        // Escape for JSONPath quoted notation, accounting for SQL string literal parsing.
                        // The JSONPath is embedded in a SQL string literal '...', so backslashes are 
                        // interpreted at the SQL level first, then at the JSONPath level.
                        // 
                        // For a property like: say "hello"
                        // JSONPath needs: $."say \"hello\""
                        // SQL literal needs: '$."say \\"hello\\""' (double backslash survives SQL parsing as single backslash)
                        var escapedName = propertyName
                            .Replace("\\", "\\\\\\\\")  // Backslash -> 4 backslashes (2 for JSONPath escape, doubled for SQL)
                            .Replace("\"", "\\\\\"")    // Double quote -> 2 backslashes + quote (escaped at both levels)
                            .Replace("'", "\\'");       // Single quote -> escaped for SQL string literal
                        jsonPathBuilder.Append($".\"{escapedName}\"");
                    }
                    else
                    {
                        jsonPathBuilder.Append($".{propertyName}");
                    }
                }
                else if (pathSegment.ArrayIndex != null)
                {
                    // For array indices: $[0] or $.property[0]
                    // Note: ArrayIndex is a SqlExpression, for constant indices this works
                    // For dynamic indices, this would need more complex handling
                    jsonPathBuilder.Append("[");
                    // Try to get constant value if possible
                    if (pathSegment.ArrayIndex is SqlConstantExpression constantExpr)
                    {
                        jsonPathBuilder.Append(constantExpr.Value);
                    }
                    else
                    {
                        // For non-constant indices, we'd need dynamic evaluation.
                        // Throw an exception to prevent silently generating a potentially incorrect query.
                        throw new NotSupportedException("The Cloud Spanner EF Core provider does not support using a non-constant index when querying a JSON array.");
                    }
                    jsonPathBuilder.Append("]");
                }
            }
            
            var jsonPath = jsonPathBuilder.ToString();
            
            // Generate: JSON_VALUE(column, '$.path')
            Sql.Append("JSON_VALUE(");
            Visit(jsonScalarExpression.Json);
            Sql.Append(", '");
            Sql.Append(jsonPath);
            Sql.Append("')");
            
            // Close the CAST if needed
            if (needsCast)
            {
                Sql.Append(" AS ");
                Sql.Append(typeMapping!.StoreType);
                Sql.Append(")");
            }
            
            return jsonScalarExpression;
        }
        
    }
}
