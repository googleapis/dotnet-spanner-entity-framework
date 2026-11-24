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

using System.Data;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class ConnectionStringBuilderTests
{
    [Test]
    public void Basic()
    {
        var builder = new SpannerConnectionStringBuilder();
        Assert.That(builder.Keys, Is.Empty);
        Assert.That(builder.Count, Is.EqualTo(0));
        Assert.That(builder.ContainsKey("host"), Is.False);
        builder.Host = "myhost";
        Assert.That(builder["host"], Is.EqualTo("myhost"));
        Assert.That(builder.Count, Is.EqualTo(1));
        Assert.That(builder.ConnectionString, Is.EqualTo("Host=myhost"));
        builder.Remove("HOST");
        Assert.That(builder["host"], Is.EqualTo(""));
        Assert.That(builder.Count, Is.EqualTo(0));
    }

    [Test]
    public void TryGetValue()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            ConnectionString = "Host=myhost"
        };
        Assert.That(builder.TryGetValue("Host", out var value), Is.True);
        Assert.That(value, Is.EqualTo("myhost"));
        Assert.That(builder.TryGetValue("SomethingUnknown", out value), Is.False);
    }

    [Test]
    public void Remove()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            UsePlainText = true
        };
        Assert.That(builder["Use plain text"], Is.True);
        builder.Remove("UsePlainText");
        Assert.That(builder.ConnectionString, Is.EqualTo(""));
    }

    [Test]
    public void Clear()
    {
        var builder = new SpannerConnectionStringBuilder { Host = "myhost" };
        builder.Clear();
        Assert.That(builder.Count, Is.EqualTo(0));
        Assert.That(builder["host"], Is.EqualTo(""));
        Assert.That(builder.Host, Is.Empty);
    }

    [Test]
    public void RemovingResetsToDefault()
    {
        var builder = new SpannerConnectionStringBuilder();
        Assert.That(builder.Port, Is.EqualTo(SpannerConnectionStringOption.Port.DefaultValue));
        builder.Port = 8;
        builder.Remove("Port");
        Assert.That(builder.Port, Is.EqualTo(SpannerConnectionStringOption.Port.DefaultValue));
    }

    [Test]
    public void SettingToNullResetsToDefault()
    {
        var builder = new SpannerConnectionStringBuilder();
        Assert.That(builder.Port, Is.EqualTo(SpannerConnectionStringOption.Port.DefaultValue));
        builder.Port = 8;
        builder["Port"] = null;
        Assert.That(builder.Port, Is.EqualTo(SpannerConnectionStringOption.Port.DefaultValue));
    }

    [Test]
    public void Enum()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            ConnectionString = "DefaultIsolationLevel=Serializable"
        };
        Assert.That(builder.DefaultIsolationLevel, Is.EqualTo(IsolationLevel.Serializable));
        Assert.That(builder.Count, Is.EqualTo(1));
    }

    [Test]
    public void EnumCaseInsensitive()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            ConnectionString = "defaultisolationlevel=repeatable read"
        };
        Assert.That(builder.DefaultIsolationLevel, Is.EqualTo(IsolationLevel.RepeatableRead));
        Assert.That(builder.Count, Is.EqualTo(1));
    }

    [Test]
    public void Clone()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            Host = "myhost"
        };
        var builder2 = builder.Clone();
        Assert.That(builder2.Host, Is.EqualTo("myhost"));
        Assert.That(builder2["Host"], Is.EqualTo("myhost"));
        Assert.That(builder.Port, Is.EqualTo(SpannerConnectionStringOption.Port.DefaultValue));
    }

    [Test]
    public void ConversionErrorThrows()
    {
        // ReSharper disable once CollectionNeverQueried.Local
        var builder = new SpannerConnectionStringBuilder();
        Assert.That(() => builder["Port"] = "hello",
            Throws.Exception.TypeOf<ArgumentException>().With.Message.Contains("Port"));
    }

    [Test]
    public void InvalidConnectionStringThrows()
    {
        var builder = new SpannerConnectionStringBuilder();
        Assert.That(() => builder.ConnectionString = "Server=127.0.0.1;User Id=npgsql_tests;Pooling:false",
            Throws.Exception.TypeOf<ArgumentException>());
    }

    [Test]
    public void ConnectionStringToProperties()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            ConnectionString = "Host=localhost;Port=80;UsePlainText=true;DefaultIsolationLevel=Repeatable read",
        };
        Assert.That(builder.Host, Is.EqualTo("localhost"));
        Assert.That(builder.Port, Is.EqualTo(80));
        Assert.That(builder.UsePlainText, Is.True);
        Assert.That(builder.DefaultIsolationLevel, Is.EqualTo(IsolationLevel.RepeatableRead));
    }

    [Test]
    public void PropertiesToConnectionString()
    {
        var builder = new SpannerConnectionStringBuilder
        {
            Host = "localhost",
            Port = 80,
            UsePlainText = true,
            DefaultIsolationLevel = IsolationLevel.RepeatableRead,
            DataSource = "projects/project1/instances/instance1/databases/database1"
        };
        Assert.That(builder.ConnectionString, Is.EqualTo("Data Source=projects/project1/instances/instance1/databases/database1;Host=localhost;Port=80;UsePlainText=True;DefaultIsolationLevel=RepeatableRead"));
        Assert.That(builder.SpannerLibConnectionString, Is.EqualTo("localhost:80/projects/project1/instances/instance1/databases/database1;UsePlainText=True;DefaultIsolationLevel=RepeatableRead"));
    }

    [TestCase("projects/p1/instances/i1/databases/d1")]
    [TestCase("projects/google.com:my-project/instances/my-instance/databases/my_db-1")]
    [TestCase("projects/DEFAULT_PROJECT_ID/instances/i1/databases/d1")]
    [TestCase("instances/i1/databases/d1")]
    [TestCase("databases/d1")]
    public void DataSource_AcceptsValidFormats(string dataSource)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder
        {
            DataSource = dataSource,
        };

        Assert.That(connectionStringBuilder.DataSource, Is.EqualTo(dataSource));
        Assert.That(connectionStringBuilder.SpannerLibConnectionString, Is.EqualTo(dataSource));
    }

    [TestCase("projects/p1/instances/i1/databases/d1;usePlainText=true")]
    [TestCase("projects/p1/instances/i1/databases/d1?usePlainText=true")]
    [TestCase("projects/P1/instances/i1/databases/d1")]
    [TestCase("projects/p1/instances/i_1/databases/d1")]
    [TestCase("instances/i1/databases/d1;usePlainText=true")]
    [TestCase("databases/d1?usePlainText=true")]
    [TestCase("evil.com:9010/projects/p1/instances/i1/databases/d1")]
    [TestCase("postgres://evil.com/projects/p1/instances/i1/databases/d1?sslmode=disable")]
    [TestCase("postgresql://evil.com/projects/p1/instances/i1/databases/d1")]
    [TestCase("projects/p1/instances/i1/databases/d1/extra")]
    [TestCase("projects/p1/instances/i1/databases/d1\n")]
    [TestCase("projects/p1/instances/i1/databases/d1 ")]
    [TestCase("projects/p1/databases/d1")]
    [TestCase("invalid")]
    public void DataSource_RejectsInjectionAndSmugglingValues(string dataSource)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder();

        Assert.Throws<ArgumentException>(() => connectionStringBuilder.DataSource = dataSource);
        Assert.Throws<ArgumentException>(() => connectionStringBuilder["DataSource"] = dataSource);
        Assert.Throws<ArgumentException>(() => _ = new SpannerConnectionStringBuilder($"Data Source=\"{dataSource}\""));
    }

    [TestCase("p1;usePlainText=true")]
    [TestCase("p1?usePlainText=true")]
    [TestCase("p1/instances/evil/databases/evil;usePlainText=true;x=")]
    [TestCase("p1=1")]
    [TestCase("p1\0")]
    [TestCase("p1 ")]
    public void Project_RejectsInvalidAndInjectionValues(string project)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder();

        Assert.Throws<ArgumentException>(() => connectionStringBuilder.Project = project);
        Assert.Throws<ArgumentException>(() => _ = new SpannerConnectionStringBuilder($"Project=\"{project}\";Instance=i1;Database=d1"));
    }

    [TestCase("i1;usePlainText=true")]
    [TestCase("i1?usePlainText=true")]
    [TestCase("i1/databases/evil;usePlainText=true;x=")]
    [TestCase("i1=1")]
    [TestCase("i1\r\n")]
    [TestCase("i1 ")]
    public void Instance_RejectsInvalidAndInjectionValues(string instance)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder();

        Assert.Throws<ArgumentException>(() => connectionStringBuilder.Instance = instance);
        Assert.Throws<ArgumentException>(() => _ = new SpannerConnectionStringBuilder($"Project=p1;Instance=\"{instance}\";Database=d1"));
    }

    [TestCase("d1;usePlainText=true")]
    [TestCase("d1?usePlainText=true")]
    [TestCase("d1/extra")]
    [TestCase("d1=1")]
    [TestCase("d1\n")]
    [TestCase("d1 ")]
    public void Database_RejectsInvalidAndInjectionValues(string database)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder();

        Assert.Throws<ArgumentException>(() => connectionStringBuilder.Database = database);
        Assert.Throws<ArgumentException>(() => connectionStringBuilder["Initial Catalog"] = database);
        Assert.Throws<ArgumentException>(() => _ = new SpannerConnectionStringBuilder($"Project=p1;Instance=i1;Database=\"{database}\""));
    }

    [Test]
    public void ProjectInstanceDatabase_BuildValidSpannerLibConnectionString()
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder
        {
            Project = "google.com:my-project",
            Instance = "my-instance",
            Database = "my_database-1",
        };

        Assert.That(connectionStringBuilder.SpannerLibConnectionString,
            Is.EqualTo("projects/google.com:my-project/instances/my-instance/databases/my_database-1"));
    }

    [TestCase("localhost", 443u, "localhost/projects/p1/instances/i1/databases/d1")]
    [TestCase("127.0.0.1", 9010u, "127.0.0.1:9010/projects/p1/instances/i1/databases/d1")]
    [TestCase("localhost:9010", 443u, "localhost:9010/projects/p1/instances/i1/databases/d1")]
    [TestCase("[::1]", 443u, "[::1]/projects/p1/instances/i1/databases/d1")]
    [TestCase("[::1]", 9010u, "[::1]:9010/projects/p1/instances/i1/databases/d1")]
    [TestCase("[::1]:9010", 443u, "[::1]:9010/projects/p1/instances/i1/databases/d1")]
    [TestCase("::1", 443u, "[::1]/projects/p1/instances/i1/databases/d1")]
    [TestCase("::1", 9010u, "[::1]:9010/projects/p1/instances/i1/databases/d1")]
    [TestCase("spanner_emulator", 9010u, "spanner_emulator:9010/projects/p1/instances/i1/databases/d1")]
    public void Host_AcceptsValidFormats(string host, uint port, string expectedDsn)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder
        {
            Host = host,
            Port = port,
            DataSource = "projects/p1/instances/i1/databases/d1",
        };

        Assert.That(connectionStringBuilder.SpannerLibConnectionString, Is.EqualTo(expectedDsn));
    }

    [TestCase("evil.com/projects/evil/instances/evil/databases/evil;usePlainText=true;x=")]
    [TestCase("postgres://evil.com/projects/evil/instances/evil/databases/evil?sslmode=disable&x=")]
    [TestCase("postgresql://evil.com/projects/evil/instances/evil/databases/evil")]
    [TestCase("localhost;usePlainText=true")]
    [TestCase("localhost?usePlainText=true")]
    [TestCase("localhost=true")]
    [TestCase("user@evil.com")]
    [TestCase("localhost#fragment")]
    [TestCase("localhost:0")]
    [TestCase("localhost:65536")]
    [TestCase("localhost:")]
    [TestCase("[::1")]
    [TestCase("[::1]:0")]
    [TestCase("[::1]:65536")]
    [TestCase("[127.0.0.1]")]
    [TestCase("::1%eth0")]
    [TestCase("[::1%eth0]")]
    [TestCase("[2001:db8::1%eth0]:9010")]
    [TestCase("-badhost")]
    [TestCase("localhost\n")]
    [TestCase("localhost ")]
    public void Host_RejectsInjectionValues(string host)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder();

        Assert.Throws<ArgumentException>(() => connectionStringBuilder.Host = host);
        Assert.Throws<ArgumentException>(() => connectionStringBuilder["Server"] = host);
        Assert.Throws<ArgumentException>(() => _ = new SpannerConnectionStringBuilder($"Host=\"{host}\";Data Source=projects/p1/instances/i1/databases/d1"));
    }

    [TestCase("localhost:443")]
    [TestCase("localhost:9010")]
    [TestCase("[::1]:9010")]
    public void HostWithPort_AndExplicitPort_ThrowsArgumentException(string host)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder
        {
            Host = host,
            Port = 80,
            DataSource = "projects/p1/instances/i1/databases/d1",
        };

        Assert.Throws<ArgumentException>(() => _ = connectionStringBuilder.SpannerLibConnectionString);
    }

    [TestCase("public;usePlainText=true")]
    [TestCase("public?usePlainText=true")]
    [TestCase("public=1")]
    [TestCase("public\r\n")]
    [TestCase("public\0")]
    public void SearchPath_RejectsInjectionValues(string searchPath)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder();

        Assert.Throws<ArgumentException>(() => connectionStringBuilder.SearchPath = searchPath);
        Assert.Throws<ArgumentException>(() => _ = new SpannerConnectionStringBuilder($"Data Source=projects/p1/instances/i1/databases/d1;SearchPath=\"{searchPath}\""));
    }

    [Test]
    public void SearchPath_AcceptsCommaSeparatedSchemas()
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder
        {
            DataSource = "projects/p1/instances/i1/databases/d1",
            SearchPath = "public, my_schema",
        };

        Assert.That(connectionStringBuilder.SpannerLibConnectionString,
            Is.EqualTo("projects/p1/instances/i1/databases/d1;search_path=public, my_schema"));
    }

    [Test]
    public void UnknownKey_IsRejectedInsteadOfForwardedVerbatim()
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder();

        Assert.Throws<ArgumentException>(() => _ = connectionStringBuilder["UnknownOption"]);
        Assert.Throws<ArgumentException>(() => connectionStringBuilder["UnknownOption"] = "value");
        Assert.Throws<ArgumentException>(() => connectionStringBuilder.Add("UnknownOption", "value"));
        Assert.Throws<ArgumentException>(() => connectionStringBuilder["a=b;usePlainText"] = "true");
        Assert.Throws<ArgumentException>(() => _ = new SpannerConnectionStringBuilder("Data Source=projects/p1/instances/i1/databases/d1;UnknownOption=\"val;usePlainText=true\""));
    }

    [Test]
    public void AllowlistedSpannerLibOption_RejectsInjectionAndForwardsValidValue()
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder("Data Source=projects/p1/instances/i1/databases/d1;retryAbortsInternally=false");
        Assert.That(connectionStringBuilder.SpannerLibConnectionString,
            Is.EqualTo("projects/p1/instances/i1/databases/d1;retry_aborts_internally=false"));

        Assert.Throws<ArgumentException>(() => connectionStringBuilder["database_role"] = "reader;usePlainText=true");
        Assert.Throws<ArgumentException>(() => connectionStringBuilder["database_role"] = "reader?usePlainText=true");
        Assert.Throws<ArgumentException>(() => connectionStringBuilder["database_role"] = "reader=admin");
        Assert.Throws<ArgumentException>(() => connectionStringBuilder["database_role"] = "reader\0");
        Assert.Throws<ArgumentException>(() => connectionStringBuilder["credentials_json"] = "abc?usePlainText=true");
        Assert.Throws<ArgumentException>(() => connectionStringBuilder["credentials_json"] = "abc;usePlainText=true");

        connectionStringBuilder["credentialsjson"] = "eyJ0eXBlIjoic2VydmljZV9hY2NvdW50In0=";
        Assert.That(connectionStringBuilder.SpannerLibConnectionString,
            Is.EqualTo("projects/p1/instances/i1/databases/d1;retry_aborts_internally=false;credentials_json=eyJ0eXBlIjoic2VydmljZV9hY2NvdW50In0="));
    }

    [TestCase("invalid_without_equals")]
    [TestCase("=value")]
    [TestCase("bad?key=value")]
    [TestCase("isolation_level=a=b")]
    [TestCase("isolation_level=serializable;bad_segment")]
    [TestCase("isolation_level=serializable\n")]
    [TestCase("credentials_json=abc?usePlainText=true")]
    [TestCase("credentialsjson=abc?usePlainText=true")]
    public void Options_RejectsMalformedOrControlValues(string options)
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder();

        Assert.Throws<ArgumentException>(() => connectionStringBuilder.Options = options);
    }

    [Test]
    public void Options_AcceptsValidSegmentsAndSkipsEmptyOptions()
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder
        {
            DataSource = "projects/p1/instances/i1/databases/d1",
            Options = "isolation_level=serializable;credentials_json=eyJ0eXBlIjoic2VydmljZV9hY2NvdW50In0=;credentialsjson=eyJ0eXBlIjoic2VydmljZV9hY2NvdW50In0=;",
        };

        Assert.That(connectionStringBuilder.SpannerLibConnectionString,
            Is.EqualTo("projects/p1/instances/i1/databases/d1;isolation_level=serializable;credentials_json=eyJ0eXBlIjoic2VydmljZV9hY2NvdW50In0=;credentialsjson=eyJ0eXBlIjoic2VydmljZV9hY2NvdW50In0="));

        connectionStringBuilder.Options = ";;";
        Assert.That(connectionStringBuilder.SpannerLibConnectionString,
            Is.EqualTo("projects/p1/instances/i1/databases/d1"));

        connectionStringBuilder.Options = "";
        Assert.That(connectionStringBuilder.SpannerLibConnectionString,
            Is.EqualTo("projects/p1/instances/i1/databases/d1"));
    }

    [Test]
    public void ValidatedOptions_CanBeClearedAndInvalidObjectTypeThrowsArgumentException()
    {
        var connectionStringBuilder = new SpannerConnectionStringBuilder
        {
            DataSource = "projects/p1/instances/i1/databases/d1",
        };
        connectionStringBuilder["Data Source"] = null;
        Assert.That(connectionStringBuilder.DataSource, Is.EqualTo(""));

        connectionStringBuilder.Project = "p1";
        connectionStringBuilder.Instance = "i1";
        connectionStringBuilder.Database = "d1";
        connectionStringBuilder.DataSource = "";
        Assert.That(connectionStringBuilder.SpannerLibConnectionString, Is.EqualTo("projects/p1/instances/i1/databases/d1"));

        Assert.Throws<ArgumentException>(() => connectionStringBuilder["Data Source"] = new object());
    }
}