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

using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using System;
using System.Linq;
using System.Net;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests;

public class SpannerMockServerFixture : IDisposable
{
    private readonly Random _random = new Random();

    private readonly IWebHost _host;

    public MockSpannerService SpannerMock { get; }
    public MockDatabaseAdminService DatabaseAdminMock { get; }
    public string Endpoint => $"localhost:{Port}";
    public string Host => "localhost";
    public int Port { get; }

    public SpannerMockServerFixture()
    {
        SpannerMock = new MockSpannerService();
        DatabaseAdminMock = new MockDatabaseAdminService();
            
        var endpoint = IPEndPoint.Parse("127.0.0.1:0");
        var builder = WebHost.CreateDefaultBuilder();
        builder.UseStartup(webHostBuilderContext => new Startup(webHostBuilderContext.Configuration, SpannerMock, DatabaseAdminMock));
        builder.ConfigureKestrel(options =>
        {
            // Setup a HTTP/2 endpoint without TLS.
            options.Listen(endpoint, o => o.Protocols = HttpProtocols.Http2);
        });
        _host = builder.Build();
        _host.Start();
        var address = _host.ServerFeatures.Get<IServerAddressesFeature>()!.Addresses.First();
        var uri = new Uri(address);
        Port = uri.Port;
    }

    public void Dispose()
    {
        _host.StopAsync().Wait();
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
