# Samples for Google Cloud Spanner Entity Framework Core
This project contains a number of samples for using Entity Framework Core in combination with Google Cloud Spanner.

The Snippets directory contains standalone samples for commonly used features of Entity Framework Core and Cloud Spanner.
Browse these samples to get an impression of how the integration works or use it as a reference for best practices when implementing your own application.

The SampleModel directory contains the data model and entity model that is used with the samples. The SpannerSampleDbContext class also contains an
example for how concurrency tokens can be implemented for Cloud Spanner.

## Running a Sample
The samples can be executed using the command `dotnet run <SampleName>` from this directory. The sample runner will automatically download and
start an instance of the Spanner emulator, create the sample database and run the sample.

### Example
```
$ dotnet run AddEntitySample
Running sample AddEntity

Starting emulator...

Added 1 singer.

Stopping emulator...
```
