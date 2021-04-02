// Copyright 2021 Google LLC
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

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Benchmarks
{
    internal class BenchmarkSampleDbContext : SpannerSampleDbContext
    {
        private readonly bool _useRealSpanner;

        private readonly string _connectionString;

        private readonly MutationUsage _mutationUsage;

        internal BenchmarkSampleDbContext(bool useRealSpanner, string connectionString) : this(useRealSpanner, connectionString, MutationUsage.ImplicitTransactions)
        {
        }

        internal BenchmarkSampleDbContext(bool useRealSpanner, string connectionString, MutationUsage mutationUsage) : base()
        {
            _useRealSpanner = useRealSpanner;
            _connectionString = connectionString;
            _mutationUsage = mutationUsage;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                var connection = _useRealSpanner ? new SpannerConnection(_connectionString) : new SpannerConnection(_connectionString, ChannelCredentials.Insecure);
                optionsBuilder
                    .UseSpanner(connection)
                    .UseMutations(_mutationUsage)
                    .UseLazyLoadingProxies();
            }
        }
    }

    public class SpannerEFCoreBenchmarks
    {
        private bool _useRealSpanner = Environment.GetEnvironmentVariable("BENCHMARK_REAL_SPANNER") != null;

        private BenchmarkServer _server;

        private SpannerSampleFixture _fixture;

        private string _connectionString;

        private long _singerId;

        private static List<Singers> DataReaderToSingersList(DbDataReader reader)
        {
            var result = new List<Singers>();
            while (reader.Read())
            {
                result.Add(RowToSinger(reader));
            }
            return result;
        }

        private static Singers RowToSinger(DbDataReader reader)
        {
            var id = reader.GetInt64(reader.GetOrdinal("SingerId"));
            var firstName = reader.GetString(reader.GetOrdinal("FirstName"));
            var lastName = reader.GetString(reader.GetOrdinal("LastName"));
            var fullName = reader.GetString(reader.GetOrdinal("FullName"));
            var birthDate = reader.IsDBNull(reader.GetOrdinal("BirthDate")) ? new DateTime() : reader.GetDateTime(reader.GetOrdinal("BirthDate"));
            var picture = reader.IsDBNull(reader.GetOrdinal("Picture")) ? null : reader.GetFieldValue<byte[]>(reader.GetOrdinal("Picture"));
            return new Singers { SingerId = id, FirstName = firstName, LastName = lastName, FullName = fullName, BirthDate = SpannerDate.FromDateTime(birthDate), Picture = picture };
        }

        [GlobalSetup]
        public void SetupServer()
        {
            if (_useRealSpanner)
            {
                _fixture = new SpannerSampleFixture();
                _connectionString = _fixture.ConnectionString;
            }
            else
            {
                _server = new BenchmarkServer();
                _connectionString = $"Data Source=projects/p1/instances/i1/databases/d1;Host={_server.Host};Port={_server.Port}";
            }
            using var connection = CreateConnection();
            _singerId = MaybeCreateSingerSpanner(connection);
            MaybeInsert100Singers(connection);
        }

        [GlobalCleanup]
        public void TeardownServer()
        {
            if (_server != null)
            {
                _server.Dispose();
            }
            if (_fixture != null)
            {
                _fixture.DisposeAsync().WaitWithUnwrappedExceptions();
            }
        }

        private SpannerConnection CreateConnection()
        {
            if (_useRealSpanner)
            {
                return new SpannerConnection(_connectionString);
            }
            else
            {
                return new SpannerConnection(_connectionString, ChannelCredentials.Insecure);
            }
        }

        private long MaybeCreateSingerSpanner(SpannerConnection connection)
        {
            var singerId = _useRealSpanner ? _fixture.RandomLong() : 1L;
            if (_useRealSpanner)
            {
                connection.RunWithRetriableTransaction(transaction =>
                {
                    using var command = connection.CreateDmlCommand("INSERT INTO Singers (SingerId, FirstName, LastName) VALUES (@id, 'Test', 'Test')", new SpannerParameterCollection
                    {
                        new SpannerParameter("id", SpannerDbType.Int64, singerId)
                    });
                    command.Transaction = transaction;
                    return command.ExecuteNonQuery();
                });
            }
            return singerId;
        }

        private long MaybeCreateSingerEF(BenchmarkSampleDbContext db)
        {
            var singerId = _useRealSpanner ? _fixture.RandomLong() : 1L;
            if (_useRealSpanner)
            {
                db.Singers.Add(new Singers
                {
                    SingerId = singerId,
                    FirstName = "Test",
                    LastName = "Test",
                });
                db.SaveChanges();
            }
            return singerId;
        }

        private void MaybeInsert100Singers(SpannerConnection connection)
        {
            if (_useRealSpanner)
            {
                var random = new Random();
                connection.RunWithRetriableTransaction(transaction =>
                {
                    var command = transaction.CreateBatchDmlCommand();
                    for (int row = 0; row < 100; row++)
                    {
                        var singerId = _fixture.RandomLong();
                        var date = new SpannerDate(random.Next(1900, 2020), random.Next(1, 13), random.Next(1, 29));
                        var firstName = _fixture.RandomString(10);
                        var lastName = _fixture.RandomString(15);
                        var picture = new byte[random.Next(1, 4097)];
                        random.NextBytes(picture);
                        command.Add("INSERT INTO Singers (SingerId, FirstName, LastName, BirthDate, Picture) VALUES (@id, @firstName, @lastName, @birthDate, @picture)", new SpannerParameterCollection
                        {
                            new SpannerParameter("id", SpannerDbType.Int64, singerId),
                            new SpannerParameter("firstName", SpannerDbType.String, firstName),
                            new SpannerParameter("lastName", SpannerDbType.String, lastName),
                            new SpannerParameter("birthDate", SpannerDbType.Date, date),
                            new SpannerParameter("picture", SpannerDbType.Bytes, picture)
                        });
                    }
                    command.ExecuteNonQuery();
                });
            }
        }

        [Benchmark]
        public Singers ReadOneRowSpanner()
        {
            using var connection = CreateConnection();
            using var command = connection.CreateSelectCommand("SELECT * FROM Singers WHERE SingerId=@id", new SpannerParameterCollection {
                new SpannerParameter("id", SpannerDbType.Int64, _singerId)
            });
            using var reader = command.ExecuteReader();
            if (reader.Read())
            {
                return RowToSinger(reader);
            }
            return null;
        }

        [Benchmark]
        public Singers ReadOneRowEF()
        {
            using var db = new BenchmarkSampleDbContext(_useRealSpanner, _connectionString);
            return db.Singers.Find(_singerId);
        }

        [Benchmark]
        public long SaveOneRowWithFetchAfterSpanner()
        {
            using var connection = CreateConnection();
            var singerId = _useRealSpanner ? _fixture.RandomLong() : 1L;
            return connection.RunWithRetriableTransaction(transaction =>
            {
                var command = transaction.CreateBatchDmlCommand();
                command.Add("INSERT INTO Singers (SingerId, FirstName, LastName, BirthDate, Picture) VALUES (@id, @firstName, @lastName, @birthDate, @picture)", new SpannerParameterCollection
                {
                    new SpannerParameter("id", SpannerDbType.Int64, singerId),
                    new SpannerParameter("firstName", SpannerDbType.String, "Pete"),
                    new SpannerParameter("lastName", SpannerDbType.String, "Allison"),
                    new SpannerParameter("birthDate", SpannerDbType.Date, new DateTime(1998, 10, 6)),
                    new SpannerParameter("picture", SpannerDbType.Bytes, new byte[] { 1, 2, 3 }),
                });
                long result = command.ExecuteNonQuery().Sum();
                var selectCommand = connection.CreateSelectCommand("SELECT FullName FROM Singers WHERE SingerId=@id", new SpannerParameterCollection
                {
                    new SpannerParameter("id", SpannerDbType.Int64, singerId),
                });
                selectCommand.Transaction = transaction;
                var fullName = selectCommand.ExecuteScalar();
                if (!"Pete Allison".Equals(fullName))
                {
                    throw new InvalidProgramException($"Received invalid full name: {fullName}");
                }
                return result;
            });
        }

        [Benchmark]
        public int SaveOneRowWithFetchAfterEF()
        {
            using var db = new BenchmarkSampleDbContext(_useRealSpanner, _connectionString);
            var singerId = _useRealSpanner ? _fixture.RandomLong() : 1L;
            var singer = new Singers
            {
                SingerId = singerId,
                FirstName = "Pete",
                LastName = "Allison",
                BirthDate = new SpannerDate(1998, 10, 6),
                Picture = new byte[] { 1, 2, 3 },
            };
            db.Singers.Add(singer);
            var result = db.SaveChanges();
            if (singer.FullName != "Pete Allison")
            {
                throw new InvalidProgramException();
            }
            return result;
        }

        [Benchmark]
        public long SaveMultipleRowsSpanner()
        {
            using var connection = CreateConnection();
            var singerId = MaybeCreateSingerSpanner(connection);
            return connection.RunWithRetriableTransaction(transaction =>
            {
                var updateCount = 0;
                for (int row = 0; row < 100; row++)
                {
                    var command = connection.CreateInsertCommand("Albums", new SpannerParameterCollection
                    {
                        new SpannerParameter("id", SpannerDbType.Int64, _useRealSpanner ? _fixture.RandomLong() : row),
                        new SpannerParameter("title", SpannerDbType.String, "Pete"),
                        new SpannerParameter("releaseDate", SpannerDbType.Date, new DateTime(1998, 10, 6)),
                        new SpannerParameter("singerId", SpannerDbType.Int64, singerId),
                    });
                    updateCount += command.ExecuteNonQuery();
                }
                return updateCount;
            });
        }

        [Benchmark]
        public long SaveMultipleRowsEF()
        {
            using var db = new BenchmarkSampleDbContext(_useRealSpanner, _connectionString);
            var singerId = MaybeCreateSingerEF(db);
            for (int row = 0; row < 100; row++)
            {
                db.Albums.Add(new Albums
                {
                    AlbumId = _useRealSpanner ? _fixture.RandomLong() : row,
                    Title = "Pete",
                    ReleaseDate = new SpannerDate(1998, 10, 6),
                    SingerId = singerId,
                });
            }
            return db.SaveChanges();
        }

        [Benchmark]
        public long SaveMultipleRowsUsingDmlSpanner()
        {
            using var connection = CreateConnection();
            var singerId = MaybeCreateSingerSpanner(connection);
            return connection.RunWithRetriableTransaction(transaction =>
            {
                var command = transaction.CreateBatchDmlCommand();
                for (int row = 0; row < 100; row++)
                {
                    command.Add("INSERT INTO Albums (AlbumId, Title, ReleaseDate, SingerId) VALUES (@id, @title, @releaseDate, @singerId)", new SpannerParameterCollection
                    {
                        new SpannerParameter("id", SpannerDbType.Int64, _useRealSpanner ? _fixture.RandomLong() : row),
                        new SpannerParameter("title", SpannerDbType.String, "Pete"),
                        new SpannerParameter("releaseDate", SpannerDbType.Date, new DateTime(1998, 10, 6)),
                        new SpannerParameter("singerId", SpannerDbType.Int64, singerId),
                    });
                }
                return command.ExecuteNonQuery().Sum();
            });
        }

        [Benchmark]
        public long SaveMultipleRowsUsingDmlEF()
        {
            using var db = new BenchmarkSampleDbContext(_useRealSpanner, _connectionString, MutationUsage.Never);
            var singerId = MaybeCreateSingerEF(db);
            for (int row = 0; row < 100; row++)
            {
                db.Albums.Add(new Albums
                {
                    AlbumId = _useRealSpanner ? _fixture.RandomLong() : row,
                    Title = "Pete",
                    ReleaseDate = new SpannerDate(1998, 10, 6),
                    SingerId = singerId,
                });
            }
            return db.SaveChanges();
        }

        [Benchmark]
        public List<Singers> SelectMultipleSingersSpanner()
        {
            using var connection = CreateConnection();
            using var command = connection.CreateSelectCommand("SELECT * FROM Singers ORDER BY LastName");
            using var reader = command.ExecuteReader();
            return DataReaderToSingersList(reader);
        }

        [Benchmark]
        public List<Singers> SelectMultipleSingersEF()
        {
            using var db = new BenchmarkSampleDbContext(_useRealSpanner, _connectionString);
            return db.Singers
                .OrderBy(s => s.LastName)
                .ToList();
        }

        [Benchmark]
        public List<Singers> SelectMultipleSingersInReadOnlyTransactionSpanner()
        {
            using var connection = CreateConnection();
            using var transaction = connection.BeginReadOnlyTransaction();
            using var command = connection.CreateSelectCommand("SELECT * FROM Singers ORDER BY LastName");
            command.Transaction = transaction;
            using var reader = command.ExecuteReader();
            return DataReaderToSingersList(reader);
        }

        [Benchmark]
        public List<Singers> SelectMultipleSingersInReadOnlyTransactionEF()
        {
            using var db = new BenchmarkSampleDbContext(_useRealSpanner, _connectionString);
            using var transaction = db.Database.BeginReadOnlyTransaction();
            return db.Singers
                .OrderBy(s => s.LastName)
                .ToList();
        }

        [Benchmark]
        public List<Singers> SelectMultipleSingersInReadWriteTransactionSpanner()
        {
            using var connection = CreateConnection();
            using var transaction = connection.BeginTransaction();
            using var command = connection.CreateSelectCommand("SELECT * FROM Singers ORDER BY LastName");
            command.Transaction = transaction;
            using var reader = command.ExecuteReader();
            var singers = DataReaderToSingersList(reader);
            transaction.Commit();
            return singers;
        }

        [Benchmark]
        public List<Singers> SelectMultipleSingersInReadWriteTransactionEF()
        {
            using var db = new BenchmarkSampleDbContext(_useRealSpanner, _connectionString);
            using var transaction = db.Database.BeginTransaction();
            var singers = db.Singers
                .OrderBy(s => s.LastName)
                .ToList();
            transaction.Commit();
            return singers;
        }
    }

    public static class BenchmarkProgram
    {
        public static void Main(string[] args)
        {
            BenchmarkRunner.Run<SpannerEFCoreBenchmarks>(DefaultConfig.Instance.WithOption(ConfigOptions.DisableOptimizationsValidator, true));
        }
    }
}
