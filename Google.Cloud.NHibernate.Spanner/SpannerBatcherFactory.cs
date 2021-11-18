using NHibernate;
using NHibernate.AdoNet;
using NHibernate.Engine;

namespace Google.Cloud.NHibernate.Spanner
{
    public class SpannerBatcherFactory : IBatcherFactory
    {
        public IBatcher CreateBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
        {
            return new SpannerBatcher(connectionManager, interceptor);
        }
    }
}