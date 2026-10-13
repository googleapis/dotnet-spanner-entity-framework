# Google.Cloud.Spanner.DataProvider.Core

`Google.Cloud.Spanner.DataProvider.Core` is the core, pure-managed ADO.NET library for Cloud Spanner. It contains all ADO.NET interfaces, connection types, command handling, and type conversion logic without bundling any multi-platform native runtime binaries.

> [!IMPORTANT]
> **Which package should you install?**
>
> - **End-User Applications**: Install **[`Google.Cloud.Spanner.DataProvider`](https://www.nuget.org/packages/Google.Cloud.Spanner.DataProvider)**. That package references this core library and bundles the necessary native `spannerlib` binaries for Linux, macOS, and Windows.
> - **Library Authors & Frameworks** (such as Entity Framework Core): Reference **`Google.Cloud.Spanner.DataProvider.Core`** to compile against the ADO.NET types without forcing native runtime binaries onto your consumers.

---

## Usage

Create a connection string using a `SpannerConnectionStringBuilder`:

```csharp
using Google.Cloud.Spanner.DataProvider;
using System.Data;

var builder = new SpannerConnectionStringBuilder
{
    Project = "my-project",
    Instance = "my-instance",
    Database = "my-database",
    DefaultIsolationLevel = IsolationLevel.ReadCommitted,
};
await using var connection = new SpannerConnection(builder.ConnectionString);
await connection.OpenAsync();
await using var command = connection.CreateCommand();
command.CommandText = "SELECT 'Hello World' as Message";
await using var reader = await command.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine($"Greeting from Spanner: {reader.GetString(0)}");
}
```

### Emulator

The driver can also connect to the Spanner Emulator. The easiest way to do this is to set the `AutoConfigEmulator` property to true. This instructs the driver to connect to the Emulator on `localhost:9010` and to automatically create the Spanner instance and database in the connection string if these do not already exist.

```csharp
using Google.Cloud.Spanner.DataProvider;
using System.Data;

var builder = new SpannerConnectionStringBuilder
{
    Project = "my-project",
    Instance = "my-instance",
    Database = "my-database",
    DefaultIsolationLevel = IsolationLevel.ReadCommitted,
    // Setting AutoConfigEmulator=true instructs the driver to connect to the Spanner emulator on 'localhost:9010',
    // and to create the instance and database on the emulator if these do not already exist.
    AutoConfigEmulator = true,
};
await using var connection = new SpannerConnection(builder.ConnectionString);
await connection.OpenAsync();
await using var command = connection.CreateCommand();
command.CommandText = "SELECT 'Hello World' as Message";
await using var reader = await command.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine($"Greeting from Spanner: {reader.GetString(0)}");
}
```

## Examples

See the [spanner-ado-net-samples](../spanner-ado-net-samples/Snippets) project for ready-to-run examples for how to use various Spanner features with this driver.
