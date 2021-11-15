
using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using NHibernate.AdoNet;
using NHibernate.Driver;
using System.Data;
using System.Data.Common;

namespace Google.Cloud.NHibernate.Spanner
{
    public class SpannerDriver : DriverBase, IEmbeddedBatcherFactoryProvider
    {
        public SpannerDriver()
        {
        }

        public override DbConnection CreateConnection()
        {
            return new SpannerRetriableConnection(new SpannerConnection());
        }

        public override DbCommand CreateCommand() => new SpannerRetriableCommand(new SpannerCommand());

        public override bool UseNamedPrefixInSql => true;
        public override bool UseNamedPrefixInParameter => true;
        public override string NamedPrefix => "@";
        public System.Type BatcherFactoryClass => typeof(GenericBatchingBatcherFactory);
    }
}