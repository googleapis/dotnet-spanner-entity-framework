# Google.Cloud.EntityFrameworkCore.Spanner
[Google Cloud Spanner](https://cloud.google.com/spanner/docs/) database provider for [Entity Framework Core](https://docs.microsoft.com/en-us/ef/core/).

This project has __Preview__ release status. Known limitations are listed in the [issues](https://github.com/cloudspannerecosystem/dotnet-spanner-entity-framework/issues?q=is%3Aissue+is%3Aopen+label%3A%22known+limitation%22) list.
All supported features have been tested and verified to work with the test configurations. There may be configurations and/or data model variations that have not
yet been covered by the tests and that show unexpected behavior. Please report any problems that you might encounter by [creating a new issue](https://github.com/cloudspannerecosystem/dotnet-spanner-entity-framework/issues/new).

# Getting started
The Entity Framework Core provider allows you to use the Entity Framework to create your database, query and update data.
To get started, install the NuGet package for `Google.Cloud.EntityFrameworkCore.Spanner` and call the "UseSpanner" method
extension to configure your DbContext with Spanner support.

## Ready to run Samples
The [Google.Cloud.EntityFrameworkCore.Spanner.Samples](Google.Cloud.EntityFrameworkCore.Spanner.Samples) project contains a number of ready to run samples.

Follow these simple steps to run a sample:
1. Clone or download this repository to your local computer.
2. Open a command prompt of your choice and navigate to the Google.Cloud.EntityFrameworkCore.Spanner.Samples project folder.
3. Execute the command `dotnet run <SampleName>`. Execute `dotnet run` to get a list of available sample names.

Browse the [Google.Cloud.EntityFrameworkCore.Spanner.Samples/Snippets](Google.Cloud.EntityFrameworkCore.Spanner.Samples/Snippets) directory to view the source code of each sample.

## Example Usage
First [set up a .NET development environment](https://cloud.google.com/dotnet/docs/setup) for Google Cloud Spanner.

The following code snippet shows how to create a DbContext for a Spanner database.

```cs
public class BloggingContext : DbContext
{
    public DbSet<Blog> Blogs { get; set; }
    public DbSet<Post> Posts { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        // Configures Entity Framework to use the specified Cloud Spanner database.
        => options.UseSpanner("Data Source=projects/my-project/instances/my-instance/databases/my-database");
}

public class Blog
{
    public int BlogId { get; set; }
    public string Url { get; set; }

    public List<Post> Posts { get; } = new List<Post>();
}

public class Post
{
    public int PostId { get; set; }
    public string Title { get; set; }
    public string Content { get; set; }

    public int BlogId { get; set; }
    public Blog Blog { get; set; }
}
```

See the [Google Cloud Spanner documentation](https://cloud.google.com/spanner/docs) for more information on how to get
started with Cloud Spanner in general, how to create an instance and a database, and how to set up authentication.

# Create Model for an Existing Database (Database-First / Scaffolding)
The Cloud Spanner EF Core provider supports [scaffolding](https://docs.microsoft.com/en-us/ef/core/managing-schemas/scaffolding?tabs=vs)
to generate a model from an existing database (database-first approach).

Use the following command to generate a model for a Cloud Spanner database:

`Scaffold-DbContext "Data Source=projects/my-project/instances/my-instance/databases/my-database" Google.Cloud.EntityFrameworkCore.Spanner`

# Database Migrations
The Cloud Spanner EF Core provider supports database migrations. See
[Migrations overview](https://docs.microsoft.com/en-us/ef/core/managing-schemas/migrations/?tabs=dotnet-core-cli)
for more background information on how to use migrations.

## Example Usage with Cloud Spanner

Follow these steps to create a Cloud Spanner database from an entity model using migrations.

### 1. Create Models
Create the entity model in code.

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
Configure the `DbContext` to use Cloud Spanner by calling `DbContextOptionsBuilder.UseSpanner(string)` with a valid Cloud Spanner connection string.

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
            optionsBuilder.UseSpanner("Data Source=projects/my-project/instances/my-instance-id/databases/my-database");
        }
    }
}
```

### 3. Migration Command:
Execute the migration commands:
1. Add-Migration "migration name"
2. Update-Database

# Type Mapping
The table below shows the mapping of Cloud Spanner data types to Clr types. It is recommended to use the default type mapping,
unless you know that the values in the database will never exceed the maximum range of another chosen Clr type. Failure to do
so for numeric types will lead to an overflow exception for `INT64` and `NUMERIC` types, and to silent loss of precision for
`FLOAT64` types.

Non-nullable primitive types can be replaced by the corresponding nullable type. That is, `bool?` can be used instead of `bool` etc.

| Spanner Type | Default Clr Type | Other Possible Clr Types |
|--------------|------------------|--------------------------|
| BOOL         | bool             |                          |
| BYTES        | byte[]           |                          |
| STRING       | string           | char, Guid, Regex        |
| INT64        | long             | int, short, byte, ulong, uint, ushort, sbyte |
| FLOAT64      | double           | float                    |
| NUMERIC      | SpannerNumeric   | decimal                  |
| DATE         | SpannerDate      |                          |
| TIMESTAMP    | DateTime         |                          |

Array types are mapped to lists by default. The corresponding Clr array type of the default base type can also be used.

| Array Type         | Default Clr Type       | Other Possible Clr Types |
|--------------------|------------------------|---------------------------
| ARRAY\<BOOL\>      | List\<bool\>           | bool[]                   |
| ARRAY\<BYTES\>     | List\<byte[]\>         | byte[][]                 |
| ARRAY\<STRING\>    | List\<string\>         | string[]                 |
| ARRAY\<INT64\>     | List\<long\>           | long[]                   |
| ARRAY\<FLOAT64\>   | List\<double\>         | double[]                 |
| ARRAY\<NUMERIC\>   | List\<SpannerNumeric\> | List\<decimal\>, SpannerNumeric[], decimal[] |
| ARRAY\<DATE\>      | List\<SpannerDate\>    | SpannerDate[]            |
| ARRAY\<TIMESTAMP\> | List\<DateTime\>       | DateTime[]               |

# Running Integration Tests

The integration tests in [Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests](Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests)
can be executed both on the Spanner emulator as well as on a real Spanner instance.

## Running Integration Tests on the Emulator

All integration tests can be executed on the emulator. Follow these steps to do so:
1. Set the environment variable `SPANNER_EMULATOR_HOST=localhost:9010` (or any other valid value if you use a custom host/port for the emulator)
2. Start a Spanner emulator. See https://cloud.google.com/spanner/docs/emulator#installing_and_running_the_emulator for more information on how to do this.
3. Navigate to the project folder Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests and execute the command `dotnet test` or run the tests from your IDE.

## Running Integration Tests on a real Spanner Instance

The integration tests can also be executed on a real Spanner instance. The tests will automatically create test databases and drop these after finishing
the tests. Follow these steps to execute the integration tests on a real Spanner instance.
1. Make sure you have enabled the Spanner API in your Google Cloud project and have set up authentication. See the [Google Cloud Spanner documentation](https://cloud.google.com/spanner/docs) for more information on how to do this.
2. Set the environment variable `TEST_SPANNER_INSTANCE` to a valid Spanner instance (e.g. `spanner-test-instance`).
3. Navigate to the project folder Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests and execute the command `dotnet test` or run the tests from your IDE.

# Specific Cloud Spanner Features

Cloud Spanner has some specific features that are not supported by other relational databases. Using these with EF Core requires you to
include some custom annotations in your model.

## Interleaved Tables
[Interleaved tables](https://cloud.google.com/spanner/docs/schema-and-data-model#creating-interleaved-tables) define a parent-child relationship
between two tables where the rows of the child table are physically stored together with the parent rows.

Use the `InterleaveInParent` extension on a child table to create an interleaved table relationship between two tables.
These relationships can be used in EF Core as if it was a foreign key relationship.

```cs
public class Singer
{
    public long SingerId { get; set; }
    public string Name { get; set; }

    public virtual ICollection<Album> Albums { get; set; }
}

public class Album
{
    public long SingerId { get; set; }
    public long AlbumId { get; set; }
    public string Title {get; set; }

    public virtual Singer Singer { get; set; }
}

protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.Entity<Album>()
        .InterleaveInParent(typeof(Singer), OnDelete.Cascade)
        .HasKey(c => new { c.SingerId, c.AlbumId });
}
```

## Commit Timestamps
Cloud Spanner can write the [commit timestamp of a transaction](https://cloud.google.com/spanner/docs/commit-timestamp)
to a column in a table. This can be used to keep track of a the creation and/or last update time of a row.

Use the `UpdateCommitTimestamp` annotation to set when a commit timestamp column should be filled. Possible values are:
* Never,
* OnUpdate,
* OnInsert,
* OnInsertAndUpdate

```cs
modelBuilder.Entity<Singer>(entity =>
{
    // Specify when the CreateAt and LastUpdatedAt columns should be updated.
    entity.Property(e => e.CreatedAt)
        .HasAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp, SpannerUpdateCommitTimestamp.OnInsert);
    entity.Property(e => e.LastUpdatedAt)
        .HasAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp, SpannerUpdateCommitTimestamp.OnUpdate);
}
```

## Generated Columns
Cloud Spanner [supports generated columns](https://cloud.google.com/spanner/docs/generated-column/how-to)
that are calculated using a deterministic expression based on other columns in the same table. These columns may not be
updated by client applications. To prevent the EF Core provider to write values to these columns, they must be marked
with `.ValueGeneratedOnAddOrUpdate()`.

Example:
```cs
modelBuilder.Entity<Singer>(entity =>
{
    // FullName is generated by Cloud Spanner on each add or update and should
    // not be included in the DML statements that are generated by Entity Framework.
    entity.Property(e => e.FullName).ValueGeneratedOnAddOrUpdate();
});

CREATE TABLE Singers (
  SingerId  STRING(36) NOT NULL,
  FirstName STRING(200),
  LastName  STRING(200) NOT NULL,
  FullName  STRING(400) NOT NULL AS (COALESCE(FirstName || ' ', '') || LastName) STORED,
) PRIMARY KEY (SingerId);
```

NOTE: Do __NOT__ add `HasComputedColumnSql()` to your model. That will override
the `ValueGeneratedOnAddOrUpdate()` and cause EF Core to try to insert a value into the generated column.

# Limitations

## Generated Values for Primary Keys
Cloud Spanner does not support sequences, identity columns, or other value generators in the database that will
generate a unique value that could be used as a primary key value. Instead, the best option is to use a client
side Guid generator for a primary key if your table does not contain a natural primary key.

## Default Values
Cloud Spanner does not support default values for columns.

# Create and publish a NuGet package locally using Visual Studio

### 1. Pack your Package from the Source Code
Open the Solution in Visual Studio, Right click on `Google.Cloud.EntityFrameworkCore.Spanner` Project and  Click on `Pack`. This will create `Google.Cloud.EntityFrameworkCore.Spanner.1.0.0-beta.nupkg` File.

### 2. Download the NuGet executable
[Download the NuGet executable](https://www.nuget.org/downloads) and copy into your local feed (e.g. C:\local-nuget-feed).

### 3. Open the command prompt
Open the Command Prompt and go to your local feed path (e.g. C:\local-nuget-feed).

### 4. Add your package into NuGet Feed
Run the command below to add your package in NuGet Feed:<br />
`nuget add path\to\source\Google.Cloud.EntityFrameworkCore.Spanner\bin\Debug|Release\Google.Cloud.EntityFrameworkCore.Spanner.1.0.0-beta.nupkg -Source C:\local-nuget-feed`

### 5. Add Package Source
In Visual Studio go to `Tools --> Options --> NuGet Package Manager --> Package Sources`
Add new Package source with Name as `Local Package` and Source as your local feed path (e.g. C:\local-nuget-feed).

NOTE: To view and/or add a reference to your package in the NuGet Feed in Visual Studio (in Manage Nuget Packages), please select `Local Package` as Package source and click on `Include prerelease`.

# Licensing

* See [LICENSE](LICENSE)

[setup]: https://cloud.google.com/dotnet/docs/setup
[projects]: https://console.cloud.google.com/project
[billing]: https://support.google.com/cloud/answer/6293499#enable-billing
[enable_api]: https://console.cloud.google.com/flows/enableapi?apiid=spanner.googleapis.com
[auth]: https://cloud.google.com/docs/authentication/getting-started
