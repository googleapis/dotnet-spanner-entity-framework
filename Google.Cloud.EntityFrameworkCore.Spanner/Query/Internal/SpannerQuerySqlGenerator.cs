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
using System.Linq.Expressions;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    public class SpannerQuerySqlGenerator : QuerySqlGenerator
    {
        public SpannerQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
            : base(dependencies)
        {
        }

        protected override void GenerateTop(SelectExpression selectExpression)
        {
        }

        protected override string GenerateOperator(SqlBinaryExpression binaryExpression)
        {
            if (binaryExpression.OperatorType == ExpressionType.Add)
            {
                if ((binaryExpression.Left.TypeMapping.StoreTypeNameBase == "STRING" || binaryExpression.Left.TypeMapping.StoreTypeNameBase == "BYTES")
                    && (binaryExpression.Right.TypeMapping.StoreTypeNameBase == "STRING" || binaryExpression.Right.TypeMapping.StoreTypeNameBase == "BYTES"))
                {
                    return "||";
                }
            }
            return base.GenerateOperator(binaryExpression);
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
                case "EXTRACT":
                    {
                        Sql.Append(sqlFunctionExpression.Name);
                        Sql.Append("(");

                        Visit(sqlFunctionExpression.Arguments[0]);

                        Sql.Append(" FROM CAST(");

                        Visit(sqlFunctionExpression.Arguments[1]);

                        Sql.Append(" AS TIMESTAMP) AT TIME ZONE '+0')");

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
    }
}
