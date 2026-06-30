// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Google.Cloud.Spanner.DataProvider.Tests;

[TestFixture]
[Category("Integration")]
public class SchemaIntegrationTests
{
    private static readonly string ConnectionString = "Host=localhost;Port=9010;Data Source=projects/integration-test/instances/integration-test/databases/integration-test;UsePlainText=true;AutoConfigEmulator=true";

    [OneTimeSetUp]
    public async Task Setup()
    {
        if (!IsEmulatorRunning())
        {
            Assert.Ignore("Spanner emulator is not running on localhost:9010");
            return;
        }

        // Open connection (which will auto-create instance and database on the emulator!)
        await using var connection = new SpannerConnection(ConnectionString);
        await connection.OpenAsync();

        // Drop existing tables if they exist to start fresh (in case of re-runs on a persistent emulator)
        try
        {
            await using var dropCmd1 = new SpannerCommand("DROP TABLE Albums", connection);
            await dropCmd1.ExecuteNonQueryAsync();
        }
        catch { }

        try
        {
            await using var dropCmd2 = new SpannerCommand("DROP TABLE Singers", connection);
            await dropCmd2.ExecuteNonQueryAsync();
        }
        catch { }

        // Create Singers and Albums tables
        await using var createSingers = new SpannerCommand(
            @"CREATE TABLE Singers (
                SingerId INT64 NOT NULL,
                FirstName STRING(1024),
                LastName STRING(1024)
            ) PRIMARY KEY (SingerId)", connection);
        await createSingers.ExecuteNonQueryAsync();

        await using var createAlbums = new SpannerCommand(
            @"CREATE TABLE Albums (
                SingerId INT64 NOT NULL,
                AlbumId INT64 NOT NULL,
                AlbumTitle STRING(1024),
                CONSTRAINT FK_Albums_Singers FOREIGN KEY(SingerId) REFERENCES Singers(SingerId)
            ) PRIMARY KEY (SingerId, AlbumId)", connection);
        await createAlbums.ExecuteNonQueryAsync();
    }

    private static bool IsEmulatorRunning()
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            socket.Connect("localhost", 9010);
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    [Test]
    public async Task TablesSchema()
    {
        await using var connection = new SpannerConnection(ConnectionString);
        await connection.OpenAsync();

        var tables = connection.GetSchema("Tables");
        Assert.That(tables, Is.Not.Null);
        var tableNames = tables.Rows.Cast<DataRow>().Select(r => (string)r["TABLE_NAME"]).ToList();
        Assert.That(tableNames, Contains.Item("Singers"));
        Assert.That(tableNames, Contains.Item("Albums"));

        // Test with restrictions
        var restrictedTables = connection.GetSchema("Tables", [null, null, "Singers"]);
        Assert.That(restrictedTables.Rows, Has.Count.EqualTo(1));
        var row = restrictedTables.Rows.Cast<DataRow>().Single();
        Assert.That(row["TABLE_NAME"], Is.EqualTo("Singers"));
        Assert.That(row["TABLE_TYPE"], Is.EqualTo("BASE TABLE"));
    }

    [Test]
    public async Task ColumnsSchema()
    {
        await using var connection = new SpannerConnection(ConnectionString);
        await connection.OpenAsync();

        var columns = connection.GetSchema("Columns", [null, null, "Singers"]);
        Assert.That(columns, Is.Not.Null);
        Assert.That(columns.Rows, Has.Count.EqualTo(3));

        var columnNames = columns.Rows.Cast<DataRow>().Select(r => (string)r["COLUMN_NAME"]).ToList();
        Assert.That(columnNames, Is.EquivalentTo(new[] { "SingerId", "FirstName", "LastName" }));

        // Check specific column details
        var singerIdCol = columns.Rows.Cast<DataRow>().Single(r => r["COLUMN_NAME"].Equals("SingerId"));
        Assert.That(singerIdCol["IS_NULLABLE"], Is.EqualTo("NO"));
        Assert.That(singerIdCol["SPANNER_TYPE"], Is.EqualTo("INT64"));

        var firstNameCol = columns.Rows.Cast<DataRow>().Single(r => r["COLUMN_NAME"].Equals("FirstName"));
        Assert.That(firstNameCol["IS_NULLABLE"], Is.EqualTo("YES"));
        Assert.That(firstNameCol["SPANNER_TYPE"], Is.EqualTo("STRING(1024)"));
    }

    [Test]
    public async Task IndexesSchema()
    {
        await using var connection = new SpannerConnection(ConnectionString);
        await connection.OpenAsync();

        var indexes = connection.GetSchema("Indexes", [null, null, "Singers"]);
        Assert.That(indexes, Is.Not.Null);
        Assert.That(indexes.Rows, Has.Count.GreaterThanOrEqualTo(1));

        var pkIndex = indexes.Rows.Cast<DataRow>().Single(r => r["INDEX_NAME"].Equals("PRIMARY_KEY"));
        Assert.That(pkIndex["INDEX_TYPE"], Is.EqualTo("PRIMARY_KEY"));
        Assert.That(pkIndex["IS_UNIQUE"], Is.True);
    }

    [Test]
    public async Task IndexColumnsSchema()
    {
        await using var connection = new SpannerConnection(ConnectionString);
        await connection.OpenAsync();

        var indexCols = connection.GetSchema("IndexColumns", [null, null, "Singers", "PRIMARY_KEY"]);
        Assert.That(indexCols, Is.Not.Null);
        Assert.That(indexCols.Rows, Has.Count.EqualTo(1));

        var row = indexCols.Rows.Cast<DataRow>().Single();
        Assert.That(row["COLUMN_NAME"], Is.EqualTo("SingerId"));
        Assert.That(row["COLUMN_ORDERING"], Is.EqualTo("ASC"));
    }

    [Test]
    public async Task ForeignKeysSchema()
    {
        await using var connection = new SpannerConnection(ConnectionString);
        await connection.OpenAsync();

        var foreignKeys = connection.GetSchema("Foreign Keys", [null, null, "Albums"]);
        Assert.That(foreignKeys, Is.Not.Null);
        Assert.That(foreignKeys.Rows, Has.Count.EqualTo(1));

        var row = foreignKeys.Rows.Cast<DataRow>().Single();
        Assert.That(row["CONSTRAINT_NAME"], Is.EqualTo("FK_Albums_Singers"));
    }
}
