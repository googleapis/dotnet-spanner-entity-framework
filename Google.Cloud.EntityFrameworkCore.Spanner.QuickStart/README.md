# Google.Cloud.EntityFrameworkCore.Spanner.QuickStart

A sample application to define a model, populate it with data, then query the
database.

## Running QuickStart

1. [Setup Cloud
   Spanner](https://cloud.google.com/spanner/docs/getting-started/set-up).
1. Create a Cloud Spanner instance and database following the [Quickstart using the Cloud Console](https://cloud.google.com/spanner/docs/quickstart-console).
1. Use the Cloud Console to create the schema defined in the [Data
   Model](https://github.com/googleapis/dotnet-spanner-entity-framework/blob/master/Google.Cloud.EntityFrameworkCore.Spanner.QuickStart/DataModel.sql).
1. Edit the [connection
   string](https://github.com/googleapis/dotnet-spanner-entity-framework/blob/master/Google.Cloud.EntityFrameworkCore.Spanner.QuickStart/QuickStart.cs#L105) to use your project, instance and database.
1. Run the sample.
```
dotnet run
```
