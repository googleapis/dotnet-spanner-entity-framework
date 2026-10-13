# Google.Cloud.Spanner.DataProvider

`Google.Cloud.Spanner.DataProvider` is the official ADO.NET Data Provider for Google Cloud Spanner backed by the high-performance `spannerlib` core.

This is the **primary package** for .NET applications connecting to Cloud Spanner via ADO.NET. It includes the complete ADO.NET API as well as the native `spannerlib` runtime binaries for supported platforms.

---

## Supported Operating Systems & Architectures

The package bundles native binaries for:
- `linux-x64`
- `linux-arm64`
- `osx-arm64`
- `win-x64`

When you publish or deploy your application (`dotnet publish -r <RID>`), the .NET SDK automatically includes only the binary for your target platform.

---

## Installation

```bash
dotnet add package Google.Cloud.Spanner.DataProvider
```

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
