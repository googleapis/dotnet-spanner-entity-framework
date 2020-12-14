using Google.Api.Gax;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
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
                Sql.AppendLine().Append($"LIMIT {int.MaxValue}");
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
