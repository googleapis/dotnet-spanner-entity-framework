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

using Google.Cloud.Spanner.Connection;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using NHibernate;
using NHibernate.AdoNet;
using NHibernate.Driver;
using NHibernate.SqlTypes;
using System;
using System.Data.Common;
using System.Threading;

namespace Google.Cloud.NHibernate.Spanner
{
    public class SpannerDriver : DriverBase, IEmbeddedBatcherFactoryProvider
    {
        private static readonly Lazy<SessionPoolManager> s_sessionPoolManager = new Lazy<SessionPoolManager>(() =>
        {
            var settings = SpannerSettings.GetDefault();
            settings.VersionHeaderBuilder.AppendAssemblyVersion("nhibernate", typeof(SpannerDriver));
            return SessionPoolManager.CreateWithSettings(new SessionPoolOptions(), settings);
        }, LazyThreadSafetyMode.ExecutionAndPublication);
        internal static SessionPoolManager SessionPoolManager { get; } = s_sessionPoolManager.Value;
        
        public SpannerDriver()
        {
        }

        public override DbConnection CreateConnection()
        {
            var connectionStringBuilder = new SpannerConnectionStringBuilder
            {
                SessionPoolManager = SessionPoolManager
            };
            var spannerConnection = new SpannerConnection(connectionStringBuilder);
            return new SpannerRetriableConnection(spannerConnection);
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