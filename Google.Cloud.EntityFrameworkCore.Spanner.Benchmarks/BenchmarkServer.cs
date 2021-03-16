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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Protobuf;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using V1 = Google.Cloud.Spanner.V1;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Benchmarks
{
    internal class BenchmarkServer : IDisposable
    {
        private readonly Random _random = new Random();

        private readonly Server _server;
        public MockSpannerService SpannerMock { get; }

        public string Endpoint
        {
            get
            {
                return $"{_server.Ports.ElementAt(0).Host}:{_server.Ports.ElementAt(0).BoundPort}";
            }
        }
        public string Host { get { return _server.Ports.ElementAt(0).Host; } }
        public int Port { get { return _server.Ports.ElementAt(0).BoundPort; } }

        public BenchmarkServer()
        {
            SpannerMock = new MockSpannerService();

            // Setup standard query results.
            var selectOneSingerEF = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}WHERE s.SingerId = @__p_0{Environment.NewLine}LIMIT 1";
            var selectOneSinger = "SELECT * FROM Singers WHERE SingerId=@id";
            AddFindSingerResult(selectOneSingerEF);
            AddFindSingerResult(selectOneSinger);

            var insertOneSingerEF = $"INSERT INTO Singers (SingerId, BirthDate, FirstName, LastName, Picture){Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4)";
            var selectFullNameEF = AddSelectSingerFullNameResult("Pete Allison", 0);
            var insertOneSinger = "INSERT INTO Singers (SingerId, FirstName, LastName, BirthDate, Picture) VALUES (@id, @firstName, @lastName, @birthDate, @picture)";
            var selectFullName = "SELECT FullName FROM Singers WHERE SingerId=@id";
            SpannerMock.AddOrUpdateStatementResult(insertOneSingerEF, StatementResult.CreateUpdateCount(1L));
            SpannerMock.AddOrUpdateStatementResult(insertOneSinger, StatementResult.CreateUpdateCount(1L));
            SpannerMock.AddOrUpdateStatementResult(selectFullName, CreateFullNameResultSet("Pete Allison"));

            for (int row = 0; row < 100; row++)
            {
                var insertAlbumEF = $"INSERT INTO Albums (AlbumId, ReleaseDate, SingerId, Title){Environment.NewLine}VALUES (@p{row*4}, @p{row*4+1}, @p{row*4+2}, @p{row*4+3})";
                SpannerMock.AddOrUpdateStatementResult(insertAlbumEF, StatementResult.CreateUpdateCount(1L));
                SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteBatchDml) + insertAlbumEF, ExecutionTime.FromMillis(0, 1));
            }
            var insertAlbum = "INSERT INTO Albums (AlbumId, Title, ReleaseDate, SingerId) VALUES (@id, @title, @releaseDate, @singerId)";
            SpannerMock.AddOrUpdateStatementResult(insertAlbum, StatementResult.CreateUpdateCount(1L));

            var selectMultipleSingers = "SELECT * FROM Singers ORDER BY LastName";
            var selectMultipleSingersEF = $"SELECT s.SingerId, s.BirthDate, s.FirstName, s.FullName, s.LastName, s.Picture{Environment.NewLine}FROM Singers AS s{Environment.NewLine}ORDER BY s.LastName";
            var singers = CreateRandomSingersResults(100);
            SpannerMock.AddOrUpdateStatementResult(selectMultipleSingersEF, singers);
            SpannerMock.AddOrUpdateStatementResult(selectMultipleSingers, singers);

            // Setup standard execution times.
            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.BatchCreateSessions), ExecutionTime.FromMillis(10, 3));
            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.BeginTransaction), ExecutionTime.FromMillis(1, 1));
            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.Commit), ExecutionTime.FromMillis(2, 2));

            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteStreamingSql) + selectOneSingerEF, ExecutionTime.FromMillis(2, 2));
            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteStreamingSql) + selectOneSinger, ExecutionTime.FromMillis(2, 2));

            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteBatchDml), ExecutionTime.FromMillis(1, 1));
            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteBatchDml) + insertOneSingerEF, ExecutionTime.FromMillis(2, 1));
            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteStreamingSql) + selectFullNameEF, ExecutionTime.FromMillis(1, 2));
            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteBatchDml) + insertOneSinger, ExecutionTime.FromMillis(2, 1));
            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteStreamingSql) + selectFullName, ExecutionTime.FromMillis(1, 2));

            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteBatchDml) + insertAlbum, ExecutionTime.FromMillis(0, 1));

            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteStreamingSql) + selectMultipleSingersEF, ExecutionTime.FromMillis(4, 2));
            SpannerMock.AddOrUpdateExecutionTime(nameof(MockSpannerService.ExecuteStreamingSql) + selectMultipleSingers, ExecutionTime.FromMillis(4, 2));

            _server = new Server
            {
                Services = {V1.Spanner.BindService(SpannerMock) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            _server.Start();
        }

        public void Dispose()
        {
            _server.ShutdownAsync().Wait();
        }

        private string AddFindSingerResult(string sql)
        {
            SpannerMock.AddOrUpdateStatementResult(sql, StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.Date, "BirthDate"),
                    Tuple.Create(V1.TypeCode.String, "FirstName"),
                    Tuple.Create(V1.TypeCode.String, "FullName"),
                    Tuple.Create(V1.TypeCode.String, "LastName"),
                    Tuple.Create(V1.TypeCode.Bytes, "Picture"),
                },
                new List<object[]>
                {
                    new object[] { 1L, null, "Alice", "Alice Morrison", "Morrison", null },
                }
            ));
            return sql;
        }

        private StatementResult CreateRandomSingersResults(int count)
        {
            var rows = new List<object[]>(count);
            for (int row = 0; row < count; row++)
            {
                var date = new SpannerDate(_random.Next(1900, 2020), _random.Next(1, 13), _random.Next(1, 29));
                var picture = new byte[_random.Next(1, 4097)];
                _random.NextBytes(picture);
                rows.Add(new object[] { 1L, date.ToString(), "Alice", "Alice Morrison", "Morrison", ByteString.CopyFrom(picture).ToBase64() });
            }

            return StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.Int64, "SingerId"),
                    Tuple.Create(V1.TypeCode.Date, "BirthDate"),
                    Tuple.Create(V1.TypeCode.String, "FirstName"),
                    Tuple.Create(V1.TypeCode.String, "FullName"),
                    Tuple.Create(V1.TypeCode.String, "LastName"),
                    Tuple.Create(V1.TypeCode.Bytes, "Picture"),
                },
                rows
            );
        }

        private string AddSelectSingerFullNameResult(string fullName, int paramIndex)
        {
            var selectFullNameSql = $"{Environment.NewLine}SELECT FullName{Environment.NewLine}FROM Singers{Environment.NewLine}WHERE  TRUE  AND SingerId = @p{paramIndex}";
            SpannerMock.AddOrUpdateStatementResult(selectFullNameSql, CreateFullNameResultSet(fullName));
            return selectFullNameSql;
        }

        private StatementResult CreateFullNameResultSet(string fullName)
            => StatementResult.CreateResultSet(
                new List<Tuple<V1.TypeCode, string>>
                {
                    Tuple.Create(V1.TypeCode.String, "FullName"),
                },
                new List<object[]>
                {
                    new object[] { fullName },
                }
            );
    }
}
