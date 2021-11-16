
using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using NHibernate;
using NHibernate.AdoNet;
using NHibernate.Driver;
using NHibernate.SqlTypes;
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
        
        protected override void InitializeParameter(DbParameter dbParam, string name, SqlType sqlType)
        {
            if (sqlType == null)
            {
                throw new QueryException($"No type assigned to parameter '{name}'");
            }

            dbParam.ParameterName = FormatNameForParameter(name);
            if (dbParam is SpannerParameter spannerParameter)
            {
                if (sqlType is SpannerSqlType spannerSqlType)
                {
                    spannerParameter.SpannerDbType = spannerSqlType.SpannerDbType;
                }
                else
                {
                    spannerParameter.DbType = sqlType.DbType;
                }
            }
        }
    }
}