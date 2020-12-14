# Google.Cloud.EntityFrameworkCore.Spanner
Google Cloud Spanner database provider for Entity Framework Core.

# Getting started
The Entity Framework Core provider allows you to use the Entity Framework
to create your database, query and update data. To get started, install
the nuget package for EntityFrameworkCore Spanner and call the "UseSpanner"
method extension to configure your DbContext with Spanner support.

## Create Model for an Existing Database

`Install-Package Microsoft.EntityFrameworkCore.Tools -Version 3.1.0`

`Install-Package Google.Cloud.EntityFrameworkCore.Spanner`

`Scaffold-DbContext "Data Source=projects/project-id/instances/instance-id/databases/database-name" Google.Cloud.EntityFrameworkCore.Spanner -o Model`
