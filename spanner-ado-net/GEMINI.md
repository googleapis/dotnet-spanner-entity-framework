# Cloud Spanner ADO.NET Data Provider Developer Guide

This developer guide provides an overview of the project structure and testing guidelines for the Cloud Spanner ADO.NET Data Provider (`Google.Cloud.Spanner.Data`).

The ADO.NET provider is an ADO.NET compliant driver that can be used with Spanner. It implements standard ADO.NET interfaces like `DbConnection`, `DbCommand`, and `DbDataReader`.

## Project Structure

The `spanner-ado-net` directory is structured as a standalone multi-project solution:

- **[spanner-ado-net/](./spanner-ado-net/)**: The core ADO.NET provider implementation (`Google.Cloud.Spanner.Data.csproj`).
- **[spanner-ado-net-tests/](./spanner-ado-net-tests/)**: Unit tests for the driver, which also leverage mock server setups.
- **[spanner-ado-net-specification-tests/](./spanner-ado-net-specification-tests/)**: Direct ADO.NET specification compliance tests.
- **[spanner-ado-net-samples/](./spanner-ado-net-samples/)**: Snippets and runnable console application samples.
- **[spanner-ado-net-samples-tests/](./spanner-ado-net-samples-tests/)**: Tests ensuring the samples compile and work correctly.
- **[spanner-ado-net-getting-started-guide/](./spanner-ado-net-getting-started-guide/)**: Code assets accompanying the Getting Started guide.
- **[spanner-ado-net-benchmarks/](./spanner-ado-net-benchmarks/)**: Microbenchmarks targeting client performance.

## Architecture: Native Go-Based gRPC Proxy

The Cloud Spanner ADO.NET provider does not communicate directly with the Spanner gRPC API in C#; instead, it uses a native helper proxy written in Go:

1. **Go Proxy Server**: The driver spawns a local background process running `spannerlib_grpc_server` (written in Go, from the [googleapis/go-sql-spanner](https://github.com/googleapis/go-sql-spanner) repository).
2. **IPC Channel**: The C# ADO.NET provider communicates with this local Go process using standard gRPC over **Unix Domain Sockets** (or TCP on platforms that do not support Unix Domain Sockets).
3. **Execution**: The local Go server is started and managed automatically at runtime by `Google.Cloud.SpannerLib.Grpc.Server` (in the `SpannerLibGrpcServer/` directory).

### Building Native Binaries

The native binaries for different platforms (`osx-arm64`, `linux-x64`, `linux-arm64`, `win-x64`) must be compiled and placed in the appropriate `runtimes/` subdirectory before packaging or running tests.

The [spanner-ado-net/build-binaries.sh](./spanner-ado-net/build-binaries.sh) script automates this process:
1. Clones the target revision of `go-sql-spanner`.
2. Compiles `spannerlib_grpc_server` executables for all platforms using Go cross-compilation.
3. Copies them to the local `runtimes/<platform>/native` directory.

To build the project with native binaries locally, you can run:
```bash
cd spanner-ado-net/spanner-ado-net
./build-binaries.sh
```

## Building the Project

You can build the ADO.NET solution using standard dotnet command line:

```bash
dotnet build spanner-ado-net/spanner-ado-net.sln
```

## Running Tests

You can run unit tests or specification tests directly using dotnet CLI:

```bash
# Run unit tests
dotnet test spanner-ado-net/spanner-ado-net-tests/spanner-ado-net-tests.csproj

# Run ADO.NET specification tests
dotnet test spanner-ado-net/spanner-ado-net-specification-tests/spanner-ado-net-specification-tests.csproj
```
