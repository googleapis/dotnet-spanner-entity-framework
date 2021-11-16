using Google.Cloud.NHibernate.Spanner.Tests.Entities;
using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Connection.MockServer;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Connection;
using NHibernate.Mapping.ByCode;
using NHibernate.Transaction;
using NHibernate.Util;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.NHibernate.Spanner.Tests
{
    // ReSharper disable once ClassNeverInstantiated.Global
    internal class TestConnectionProvider : ConnectionProvider
    {
        public override DbConnection GetConnection(string connectionString)
            => new SpannerRetriableConnection(new SpannerConnection(connectionString, ChannelCredentials.Insecure));

        public override Task<DbConnection> GetConnectionAsync(string connectionString,
            CancellationToken cancellationToken)
            => Task.FromResult(GetConnection(connectionString));
    }
    
    public class NHibernateMockServerFixture : SpannerMockServerFixture
    {
        public NHibernateMockServerFixture()
        {
            ReflectHelper.ClassForName(typeof(SpannerDriver).AssemblyQualifiedName);
            var nhConfig = new Configuration().DataBaseIntegration(db =>
            {
                db.Dialect<SpannerDialect>();
                db.ConnectionString = ConnectionString;
                db.BatchSize = 100;
                db.ConnectionProvider<TestConnectionProvider>();
            });
            var mapper = new ModelMapper();
            mapper.AddMapping<SingerMapping>();
            mapper.AddMapping<TableWithAllColumnTypesMapping>();
            var mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            nhConfig.AddMapping(mapping);
            
            SessionFactory = nhConfig.BuildSessionFactory();
        }

        public ISessionFactory SessionFactory { get; }
        
        public string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={Host};Port={Port}";
    }
}