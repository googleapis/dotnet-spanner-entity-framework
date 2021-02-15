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

# Create Model for an Existing Database

1. Create Instance using [Create Instance Sample](https://github.com/GoogleCloudPlatform/dotnet-docs-samples/blob/master/spanner/api/Spanner.Samples/CreateInstance.cs).

2. Create Database using [Create Database Sample](https://github.com/GoogleCloudPlatform/dotnet-docs-samples/blob/master/spanner/api/Spanner.Samples/CreateDatabaseAsync.cs).

3. `Install-Package Microsoft.EntityFrameworkCore.Tools -Version 3.1.0`

4. `Install-Package Google.Cloud.EntityFrameworkCore.Spanner`

5. Select `Google.Cloud.EntityFrameworkCore.Spanner` as the Default project in the Package Manager Console.

6. `Scaffold-DbContext "Data Source=projects/project-id/instances/instance-id/databases/database-name" Google.Cloud.EntityFrameworkCore.Spanner -o Model -Force -Context SpannerSampleDbContext`

# Running Sample Integration Tests
The sample integration tests require a valid sample database to be executed. This sample database can be automatically
created by the integration tests, or the integration tests can be executed against an already existing database. The
latter will make the integration tests significantly faster and is the recommended way of working if you intend to
execute the tests several times. Letting the integration tests generate the database requires less manual setup, but
will make the execution time longer.

## Letting the Integration Tests Generate the Database
In order to execute the integration tests without an existing database you will need to setup a couple of environment
variables, and you need an existing Cloud Spanner instance (not database). Configure the following:
1. Setup default authentication: https://cloud.google.com/spanner/docs/getting-started/set-up#set_up_authentication_and_authorization
2. Create an environment variable TEST_PROJECT that references your Google Cloud Project.
3. Create an environment variable SPANNER_TEST_INSTANCE that references your existing Cloud Spanner instance. Only include the instance id (i.e. something like 'test-instance' and NOT 'projects/project-id/instances/test-instance')

Run the integration tests. The tests will automatically create a database for the test run and drop this database after the tests have finished.

## Using an Existing Database for the Integration Tests
In order to use an existing database that can be reused for the integration tests you need to setup the following in addition to the steps above:
1. Setup everything described above for 'Letting the Integration Tests Generate the Database'
2. Create a Cloud Spanner database and create the data model that can be found in the SampleDataModel.sql file in the root of the integration tests project.
3. Create an environment variable SPANNER_TEST_DATABASE that references this database. Only include the database id (i.e. something like 'test-database' and not 'projects/project-id/instances/test-instance/databases/test-database').

Run the integration tests. The tests will use the existing database and **not** drop it after the tests have finished. **Any data in the test tables will be lost**.

## Running the Integration Tests Agains the Emulator
**NOTE:** The sample integration tests can currently **NOT** be executed on the emulator as these use several features that are not yet supported
on the emulator:
* Computed columns
* Numeric data type
* Foreign keys

The SpannerFixtureBase class **does support** the emulator. That means that any other integration tests that extend SpannerFixtureBase
can be executed on the emulator. A simple example of an integration test that does work with the emulator can be found in BasicsTest.cs.

The BasicsTests can be executed on the emulator by following these steps:
1. Make sure that the environment variable SPANNER_TEST_DATABASE has **not** been set.
2. Set the environment variable SPANNER_EMULATOR_HOST to `localhost:9010`.
3. Start the Spanner emulator using the following command in Windows Power Shell: `gcloud beta emulators spanner start`
4. Execute the integration test. The integration test environment will automatically create an instance and a database on the emulator.

# Migrations Overview
### 1. Create Models
```cs
public partial class Singer
{
    public Singer()
    {
        Albums = new HashSet<Album>();
    }

    public long SingerId { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }

    public virtual ICollection<Album> Albums { get; set; }
}

public partial class Album
{
    public long AlbumId { get; set; }
    public string Title { get; set; }
    public DateTime? ReleaseDate { get; set; }
    public long SingerId { get; set; }

    public virtual Singer Singer { get; set; }
}
```
### 2. Configure `DbContext`

```cs
public partial class ArtistDbContext : DbContext
{
    public ArtistDbContext()
    {
    }

    public ArtistDbContext(DbContextOptions<ArtistDbContext> options)
        : base(options)
    {
    }

    public virtual DbSet<Album> Albums { get; set; }
    public virtual DbSet<Singer> Singers { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (!optionsBuilder.IsConfigured)
        {
            optionsBuilder.UseSpanner("Data Source=projects/project-id/instances/instance-id/databases/database-name");
        }
    }
}
```

### 3. Migration Command:
1. Add-Migration "migration name"
2. Update-Database

## Creating a hierarchy of interleaved tables
Using `InterleaveInParent` Attribute you can create Hierarchy of [interleaved tables][inter-leaved-table].
while declaring the Interleaved Table option it automatically ignore the foreign key referece.

```cs
public class Author
{
    public long AuthorId { get; set; }
    public string AutherName { get; set; }

    public ICollection<Article> Articles { get; set; }
}

[InterleaveInParent(typeof(Author))]
public class Article
{
    public long AuthorId { get; set; }
    public long ArticleId { get; set; }
    public DateTime PublishDate { get; set; }
    public string ArticleTitle { get; set; }
    public string ArticleContent { get; set; }
    public Author Author { get; set; }
}
```
# Query Limitations
* Operation on `ARRAY` types performs in memory.
* Data Annotation Validation on `ARRAY` types might not work. 

# Update Limitations
* Cloud Spanner does not have database value generators or constraints.
Instead, you may use a client side Guid generator for a primary key.

## Licensing

* See [LICENSE](LICENSE)

[projects]: https://console.cloud.google.com/project
[billing]: https://support.google.com/cloud/answer/6293499#enable-billing
[enable_api]: https://console.cloud.google.com/flows/enableapi?apiid=spanner.googleapis.com
[auth]: https://cloud.google.com/docs/authentication/getting-started
[inter-leaved-table]: https://cloud.google.com/spanner/docs/schema-and-data-model#creating_a_hierarchy_of_interleaved_tables