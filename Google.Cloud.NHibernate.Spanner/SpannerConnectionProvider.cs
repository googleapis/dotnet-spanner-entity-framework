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
using Grpc.Core;
using NHibernate.Connection;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.NHibernate.Spanner
{
    public class SpannerConnectionProvider : ConnectionProvider
    {
        public virtual ChannelCredentials ChannelCredentials { get; set; }

        public override DbConnection GetConnection(string connectionString)
        {
            var connectionStringBuilder = new SpannerConnectionStringBuilder(connectionString, ChannelCredentials)
            {
                SessionPoolManager = SpannerDriver.SessionPoolManager,
            };
            var spannerConnection = new SpannerConnection(connectionStringBuilder);
            return new SpannerRetriableConnection(spannerConnection);
        }

        public override Task<DbConnection> GetConnectionAsync(string connectionString,
            CancellationToken cancellationToken)
            => Task.FromResult(GetConnection(connectionString));
    }
}