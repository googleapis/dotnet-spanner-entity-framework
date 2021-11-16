using Google.Api.Gax;
using Google.Cloud.Spanner.Data;
using NHibernate.SqlTypes;
using System.Data;

namespace Google.Cloud.NHibernate.Spanner
{
    public class SpannerSqlType : SqlType
    {
        public SpannerDbType SpannerDbType { get; }
        
        public SpannerSqlType(SpannerDbType spannerDbType) : base(spannerDbType.DbType)
        {
            SpannerDbType = GaxPreconditions.CheckNotNull(spannerDbType, nameof(spannerDbType));
        }
    }
}