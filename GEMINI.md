# Cloud Spanner EF Core Provider Developer Guide

This developer guide provides an overview of the project structure, build/test guidelines, and key design concepts for developers working on the Google Cloud Spanner provider for Entity Framework Core.

## Project Structure

The repository is structured into the following key folders and projects:

- **[Google.Cloud.EntityFrameworkCore.Spanner](./Google.Cloud.EntityFrameworkCore.Spanner/)**: The main Entity Framework Core provider library.
  - **[Query/](./Google.Cloud.EntityFrameworkCore.Spanner/Query/)**: Translates LINQ expressions and EF Core query representations to Spanner-compatible SQL.
  - **[Migrations/](./Google.Cloud.EntityFrameworkCore.Spanner/Migrations/)**: Generates Spanner-compatible DDL for database migrations.
  - **[Scaffolding/](./Google.Cloud.EntityFrameworkCore.Spanner/Scaffolding/)**: Scaffolds an Entity Framework Core model from an existing Spanner database schema (Database-First).
  - **[Storage/](./Google.Cloud.EntityFrameworkCore.Spanner/Storage/)**: Configures connection/transaction management and handles type mappings.
- **[Google.Cloud.EntityFrameworkCore.Spanner.Tests](./Google.Cloud.EntityFrameworkCore.Spanner.Tests/)**: Unit tests for the provider.
  - Many of the unit tests run against an in-process Spanner mock gRPC server (`MockSpannerServer.cs`). This allows validating the EF Core generated SQL queries and transaction flows without external database dependencies.
- **[Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests](./Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests/)**: Integration tests.
  - These tests run end-to-end scenarios against either the Spanner Emulator or a real Cloud Spanner instance.
- **[Google.Cloud.EntityFrameworkCore.Spanner.Samples](./Google.Cloud.EntityFrameworkCore.Spanner.Samples/)**: Runnable samples demonstrating typical usage and Spanner-specific features.

## Building the Project

Ensure you have the .NET 10 SDK installed. You can build the solution with:

```bash
dotnet build Google.Cloud.EntityFrameworkCore.Spanner.sln
```

## Running Tests

### Unit Tests

Unit tests are fast and run against an in-process mock server:

```bash
dotnet test Google.Cloud.EntityFrameworkCore.Spanner.Tests
```

### Integration Tests

Integration tests can run either using the Spanner Emulator or a real Spanner instance.

#### 1. Running on the Spanner Emulator (Recommended)

1. Set the emulator host environment variable:
   ```bash
   export SPANNER_EMULATOR_HOST=localhost:9010
   ```
2. Start the Spanner emulator (e.g. via Docker or the `gcloud CLI`).
3. Run the integration tests:
   ```bash
   dotnet test Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
   ```

#### 2. Running on a Real Cloud Spanner Instance

1. Set the target Spanner instance name in the environment:
   ```bash
   export TEST_SPANNER_INSTANCE=my-spanner-instance
   ```
2. Ensure you are authenticated with Google Cloud SDK (`gcloud auth application-default login`).
3. Run the integration tests:
   ```bash
   dotnet test Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
   ```
   *Note: Integration tests will automatically create temporary databases and drop them upon completion.*

## Key Design Patterns & Concepts

### Aborted Transaction Retries

Cloud Spanner transactions can be aborted at any time by the Spanner backend due to lock contention or other transient errors. The provider handles these aborts automatically:

1. **Recording Statements**: The provider monitors and records all statements (queries, DMLs, Batch DMLs) executed during the transaction lifecycle.
2. **Checksum Validation**: For query (`SELECT`) statements, the provider tracks a running checksum of the returned rows.
3. **Automatic Replay**: If the transaction is aborted by the backend, the provider transparently starts a new read/write transaction and replays all recorded statements.
4. **Consistency Verification**: If the results of the replayed statements match the original results (based on DML update counts or query checksums), the transaction continues seamlessly. If results differ, the retry fails.

This transaction retry wrapper is implemented in `SpannerRetriableConnection` and `SpannerRetriableTransaction`.
