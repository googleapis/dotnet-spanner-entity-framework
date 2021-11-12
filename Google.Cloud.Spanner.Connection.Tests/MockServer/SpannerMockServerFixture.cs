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

using Grpc.Core;
using System;
using System.Linq;

namespace Google.Cloud.Spanner.Connection.MockServer
{
    public class SpannerMockServerFixture : IDisposable
    {
        private readonly Random _random = new Random();

        private readonly Server _server;
        public MockSpannerService SpannerMock { get; }
        public MockDatabaseAdminService DatabaseAdminMock { get; }
        public string Endpoint
        {
            get
            {
                return $"{_server.Ports.ElementAt(0).Host}:{_server.Ports.ElementAt(0).BoundPort}";
            }
        }
        public string Host { get { return _server.Ports.ElementAt(0).Host; } }
        public int Port { get { return _server.Ports.ElementAt(0).BoundPort; } }

        public SpannerMockServerFixture()
        {
            SpannerMock = new MockSpannerService();
            DatabaseAdminMock = new MockDatabaseAdminService();
            _server = new Server
            {
                Services = { Google.Cloud.Spanner.V1.Spanner.BindService(SpannerMock), Google.Cloud.Spanner.Admin.Database.V1.DatabaseAdmin.BindService(DatabaseAdminMock) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            _server.Start();
        }

        public void Dispose()
        {
            _server.ShutdownAsync().Wait();
        }

        public long RandomLong()
        {
            return RandomLong(0, long.MaxValue);
        }

        public long RandomLong(long min, long max)
        {
            byte[] buf = new byte[8];
            _random.NextBytes(buf);
            long longRand = BitConverter.ToInt64(buf, 0);
            return (Math.Abs(longRand % (max - min)) + min);
        }
    }
}