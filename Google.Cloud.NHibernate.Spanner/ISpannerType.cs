using Google.Cloud.Spanner.Data;

namespace Google.Cloud.NHibernate.Spanner
{
    public interface ISpannerType
    {
        SpannerDbType GetSpannerDbType();
    }
}