using Microsoft.EntityFrameworkCore.Query;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Query.Internal
{
    public class SpannerQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
    {
        private readonly QuerySqlGeneratorDependencies _dependencies;

        public SpannerQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
        {
            _dependencies = dependencies;
        }

        public virtual QuerySqlGenerator Create()
            => new SpannerQuerySqlGenerator(_dependencies);
    }
}
