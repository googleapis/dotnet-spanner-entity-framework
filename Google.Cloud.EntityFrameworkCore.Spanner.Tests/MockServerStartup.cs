// Copyright 2022 Google LLC
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

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests;

public class MockServerStartup
{
    public IConfiguration Configuration { get; }
        
    private MockSpannerService MockSpannerService { get; }

    private MockDatabaseAdminService MockDatabaseAdminService { get; }
        
    public MockServerStartup(IConfiguration configuration, MockSpannerService mockSpannerService, MockDatabaseAdminService mockDatabaseAdminService)
    {
        Configuration = configuration;
        MockSpannerService = mockSpannerService;
        MockDatabaseAdminService = mockDatabaseAdminService;
    }

    public void ConfigureServices(IServiceCollection services)
    {
        services.AddGrpc();
        services.AddSingleton(MockSpannerService);
        services.AddSingleton(MockDatabaseAdminService);
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }
        app.UseRouting();
        app.UseEndpoints(endpoints =>
        {
            endpoints.MapGrpcService<MockSpannerService>();
            endpoints.MapGrpcService<MockDatabaseAdminService>();
        });
    }
}
    