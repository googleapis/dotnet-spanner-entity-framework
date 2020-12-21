# Google.Cloud.EntityFrameworkCore.Spanner
[Google Cloud Spanner](https://cloud.google.com/spanner/docs/) database provider for Entity Framework Core.

# Getting started
The Entity Framework Core provider allows you to use the Entity Framework
to create your database, query and update data. To get started, install
the nuget package for EntityFrameworkCore Spanner and call the "UseSpanner"
method extension to configure your DbContext with Spanner support.

## Before you begin

1.  [Select or create a Cloud Platform project][projects].
1.  [Enable billing for your project][billing].
1.  [Enable the Cloud Spanner API][enable_api].
1.  [Set up authentication with a service account][auth] so you can access the
    API from your local workstation.

## Create Model for an Existing Database

1. Create Instance using [Create Instance Sample](https://github.com/GoogleCloudPlatform/dotnet-docs-samples/blob/master/spanner/api/Spanner.Samples/CreateInstance.cs).

2. Create Database using [Create Database Sample](https://github.com/GoogleCloudPlatform/dotnet-docs-samples/blob/master/spanner/api/Spanner.Samples/CreateDatabaseAsync.cs).

3. `Install-Package Microsoft.EntityFrameworkCore.Tools -Version 3.1.0`

4. `Install-Package Google.Cloud.EntityFrameworkCore.Spanner`

5. Select `Google.Cloud.EntityFrameworkCore.Spanner` as the Default project in the Package Manager Console.

6. `Scaffold-DbContext "Data Source=projects/project-id/instances/instance-id/databases/database-name" Google.Cloud.EntityFrameworkCore.Spanner -o Model -Force -Context SpannerSampleDbContext`

[projects]: https://console.cloud.google.com/project
[billing]: https://support.google.com/cloud/answer/6293499#enable-billing
[enable_api]: https://console.cloud.google.com/flows/enableapi?apiid=spanner.googleapis.com
[auth]: https://cloud.google.com/docs/authentication/getting-started