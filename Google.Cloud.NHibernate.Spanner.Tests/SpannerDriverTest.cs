using Google.Cloud.EntityFrameworkCore.Spanner.Tests;
using Google.Cloud.NHibernate.Spanner;
using Google.Cloud.NHibernate.Spanner.Tests.Entities;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using NHibernate.Cfg;
using NHibernate.Cfg.MappingSchema;
using NHibernate.Connection;
using NHibernate.Criterion;
using NHibernate.Mapping.ByCode;
using NHibernate.Util;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.NHibernate.Spanner.Tests
{
    public class SpannerDriverTest : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public SpannerDriverTest(SpannerMockServerFixture service)
        {
            _fixture = service;
            service.SpannerMock.Reset();
        }
        
        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        internal class TestConnectionProvider : ConnectionProvider
        {
            public override DbConnection GetConnection(string connectionString)
                => new SpannerConnection(connectionString, ChannelCredentials.Insecure);

            public override Task<DbConnection> GetConnectionAsync(string connectionString,
                CancellationToken cancellationToken)
                    => Task.FromResult(GetConnection(connectionString));
        }

        [Fact]
        public async Task TestConnect()
        {
            SpannerDriver driver = new SpannerDriver();
            ReflectHelper.ClassForName(typeof(SpannerDriver).AssemblyQualifiedName);
            var nhConfig = new Configuration().DataBaseIntegration(db =>
            {
                db.Dialect<SpannerDialect>();
                db.ConnectionString = ConnectionString;
                db.BatchSize = 100;
                db.ConnectionProvider<TestConnectionProvider>();
            });
            var mapper = new ModelMapper();
            mapper.AddMapping<SingerMap>();
            //mapper.AddMappings(Assembly.GetExecutingAssembly().GetExportedTypes());
            HbmMapping mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            nhConfig.AddMapping(mapping);
            
            var sessionFactory = nhConfig.BuildSessionFactory();
            using var session = sessionFactory.OpenSession();

            AddSingerResult("SELECT singer0_.SingerId as singerid1_0_0_, singer0_.FirstName as firstname2_0_0_, singer0_.LastName as lastname3_0_0_ FROM Singer singer0_ WHERE singer0_.SingerId=@p0");
            var singer = await session.LoadAsync<Singer>(1L);
            Assert.NotNull(singer);
            Assert.Equal("Alice", singer.FirstName);
        }
        
        private string AddSingerResult(string sql)
        {
            _fixture.SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "singerid1_0_0_"),
                    Tuple.Create(V1.TypeCode.Date, "BirthDate"),
                    Tuple.Create(V1.TypeCode.String, "firstname2_0_0_"),
                    Tuple.Create(V1.TypeCode.String, "FullName"),
                    Tuple.Create(V1.TypeCode.String, "lastname3_0_0_"),
                    Tuple.Create(V1.TypeCode.Bytes, "Picture"),
                },
                new List<object[]>
                {
                    new object[] { 1L, null, "Alice", "Alice Morrison", "Morrison", null },
                }
            ));
            return sql;
        }
    }
}