// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.NHibernate.Spanner.Tests.Entities;
using Google.Cloud.Spanner.Connection.MockServer;
using Grpc.Core;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Mapping.ByCode;
using NHibernate.Util;
using System;

namespace Google.Cloud.NHibernate.Spanner.Tests
{
    // ReSharper disable once ClassNeverInstantiated.Global
    internal class TestConnectionProvider : SpannerConnectionProvider
    {
        public override ChannelCredentials ChannelCredentials { get => ChannelCredentials.Insecure; set => throw new InvalidOperationException(); }
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
            mapper.AddMapping<AlbumMapping>();
            mapper.AddMapping<TableWithAllColumnTypesMapping>();
            var mapping = mapper.CompileMappingForAllExplicitlyAddedEntities();
            nhConfig.AddMapping(mapping);
            
            SessionFactory = nhConfig.BuildSessionFactory();
        }

        public ISessionFactory SessionFactory { get; }
        
        public string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={Host};Port={Port}";
    }
}