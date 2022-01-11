using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests;

public class Startup
{
    public IConfiguration Configuration { get; }
        
    private MockSpannerService MockSpannerService { get; }

    private MockDatabaseAdminService MockDatabaseAdminService { get; }
        
    public Startup(IConfiguration configuration, MockSpannerService mockSpannerService, MockDatabaseAdminService mockDatabaseAdminService)
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
        // services.AddSingleton(Google.Cloud.Spanner.V1.Spanner.BindService(MockSpannerService));
        // services.AddSingleton(Google.Cloud.Spanner.Admin.Database.V1.DatabaseAdmin.BindService(MockDatabaseAdminService));
    }

    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }
        // app.UseHttpsRedirection();
        app.UseRouting();
        app.UseEndpoints(endpoints =>
        {
            endpoints.MapGrpcService<MockSpannerService>();
            endpoints.MapGrpcService<MockDatabaseAdminService>();
        });
    }
}
    