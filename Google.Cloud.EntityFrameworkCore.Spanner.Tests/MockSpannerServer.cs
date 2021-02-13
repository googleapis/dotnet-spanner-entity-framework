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

using Google.Cloud.Spanner.Common.V1;
using V1 = Google.Cloud.Spanner.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Status = Google.Rpc.Status;
using Google.Cloud.Spanner.V1;
using Google.Cloud.Spanner.Data;
using System.Reflection;
using Google.Rpc;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
    public class StatementResult
    {
        public enum StatementResultType
        {
            RESULT_SET,
            UPDATE_COUNT,
            EXCEPTION
        }

        public StatementResultType Type { get; }
        public ResultSet ResultSet { get; }
        public long UpdateCount { get; }
        public Exception Exception { get; }

        internal static StatementResult CreateQuery(ResultSet resultSet)
        {
            return new StatementResult(resultSet);
        }

        internal static StatementResult CreateUpdateCount(long count)
        {
            return new StatementResult(count);
        }

        internal static StatementResult CreateException(Exception exception)
        {
            return new StatementResult(exception);
        }

        internal static StatementResult CreateSelect1ResultSet()
        {
            return CreateSingleColumnResultSet(new V1.Type { Code = V1.TypeCode.Int64 }, "COL1", 1);
        }

        internal static StatementResult CreateSingleColumnResultSet(V1.Type type, string col, params object[] values)
        {
            ResultSet rs = new ResultSet
            {
                Metadata = new ResultSetMetadata
                {
                    RowType = new StructType()
                },
            };
            rs.Metadata.RowType.Fields.Add(new StructType.Types.Field
            {
                Name = col,
                Type = type,
            });
            foreach (object val in values)
            {
                ListValue row = new ListValue();
                row.Values.Add(SpannerConverter.ToProtobufValue(type, val));
                rs.Rows.Add(row);
            }
            return CreateQuery(rs);
        }

        internal static StatementResult CreateResultSet(IEnumerable<Tuple<V1.TypeCode, string>> columns, IEnumerable<object[]> rows) =>
            CreateResultSet(columns.Select(x => Tuple.Create(new V1.Type { Code = x.Item1 }, x.Item2)).ToList(), rows);

        internal static StatementResult CreateResultSet(IEnumerable<Tuple<V1.Type, string>> columns, IEnumerable<object[]> rows)
        {
            var rs = new ResultSet
            {
                Metadata = new ResultSetMetadata
                {
                    RowType = new StructType()
                },
            };
            foreach (var col in columns)
            {
                rs.Metadata.RowType.Fields.Add(new StructType.Types.Field
                {
                    Type = col.Item1,
                    Name = col.Item2,
                });
            }
            foreach (var rowValue in rows)
            {
                    var row = new ListValue();
                var colIndex = 0;
                foreach (var value in rowValue)
                {
                    row.Values.Add(SpannerConverter.ToProtobufValue(rs.Metadata.RowType.Fields[colIndex].Type, value));
                    colIndex++;
                }
                rs.Rows.Add(row);
            }
            return CreateQuery(rs);
        }

        private StatementResult(ResultSet resultSet)
        {
            Type = StatementResultType.RESULT_SET;
            ResultSet = resultSet;
        }

        private StatementResult(long updateCount)
        {
            Type = StatementResultType.UPDATE_COUNT;
            UpdateCount = updateCount;
        }

        private StatementResult(Exception exception)
        {
            Type = StatementResultType.EXCEPTION;
            Exception = exception;
        }
    }

    public class ExecutionTime
    {
        private readonly int _executionTime;
        // TODO: Support multiple exceptions
        private Exception _exception;
        private readonly int _exceptionStreamIndex;

        internal bool HasExceptionAtIndex(int index)
        {
            return _exception != null && _exceptionStreamIndex == index;
        }

        internal Exception PopExceptionAtIndex(int index)
        {
            Exception res = _exceptionStreamIndex == index ? _exception : null;
            if (res != null)
            {
                _exception = null;
            }
            return res;
        }

        internal static ExecutionTime StreamException(Exception exception, int streamIndex)
        {
            return new ExecutionTime(0, exception, streamIndex);
        }

        private ExecutionTime(int executionTime, Exception exception, int exceptionStreamIndex)
        {
            _executionTime = executionTime;
            _exception = exception;
            _exceptionStreamIndex = exceptionStreamIndex;
        }
    }

    public class MockSpannerService : V1.Spanner.SpannerBase
    {
        private class PartialResultSetsEnumerable : IEnumerable<PartialResultSet>
        {
            private readonly ResultSet _resultSet;
            public PartialResultSetsEnumerable(ResultSet resultSet)
            {
                this._resultSet = resultSet;
            }

            public IEnumerator<PartialResultSet> GetEnumerator()
            {
                return new PartialResultSetsEnumerator(_resultSet);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new PartialResultSetsEnumerator(_resultSet);
            }
        }

        private class PartialResultSetsEnumerator : IEnumerator<PartialResultSet>
        {
            private static readonly int MAX_ROWS_IN_CHUNK = 1;

            private readonly ResultSet _resultSet;
            private bool _first = true;
            private int _currentRow = 0;
            private PartialResultSet _current;

            public PartialResultSetsEnumerator(ResultSet resultSet)
            {
                _resultSet = resultSet;
            }

            PartialResultSet IEnumerator<PartialResultSet>.Current => _current;

            object IEnumerator.Current => _current;

            public bool MoveNext()
            {
                _current = new PartialResultSet
                {
                    ResumeToken = ByteString.CopyFromUtf8($"{_currentRow}")
                };
                if (_first)
                {
                    _current.Metadata = _resultSet.Metadata;
                    _first = false;
                }
                else if (_currentRow == _resultSet.Rows.Count)
                {
                    return false;
                }
                int recordCount = 0;
                while (recordCount < MAX_ROWS_IN_CHUNK && _currentRow < _resultSet.Rows.Count)
                {
                    _current.Values.Add(_resultSet.Rows.ElementAt(_currentRow).Values);
                    recordCount++;
                    _currentRow++;
                }
                return true;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public void Dispose()
            {
            }
        }

        private static readonly Empty EMPTY = new Empty();
        private static readonly TransactionOptions SINGLE_USE = new TransactionOptions { ReadOnly = new TransactionOptions.Types.ReadOnly { Strong = true, ReturnReadTimestamp = false } };

        private readonly object _lock = new object();
        private readonly ConcurrentDictionary<string, StatementResult> _results = new ConcurrentDictionary<string, StatementResult>();
        private ConcurrentQueue<IMessage> _requests = new ConcurrentQueue<IMessage>();
        private int _sessionCounter;
        private int _transactionCounter;
        private readonly ConcurrentDictionary<SessionName, Session> _sessions = new ConcurrentDictionary<SessionName, Session>();
        private readonly ConcurrentDictionary<ByteString, Transaction> _transactions = new ConcurrentDictionary<ByteString, Transaction>();
        private readonly ConcurrentDictionary<ByteString, TransactionOptions> _transactionOptions = new ConcurrentDictionary<ByteString, TransactionOptions>();
        private readonly ConcurrentDictionary<ByteString, bool> _abortedTransactions = new ConcurrentDictionary<ByteString, bool>();
        private bool _abortNextStatement = false;
        private readonly ConcurrentDictionary<string, ExecutionTime> _executionTimes = new ConcurrentDictionary<string, ExecutionTime>();

        public void AddOrUpdateStatementResult(string sql, StatementResult result)
        {
            _results.AddOrUpdate(sql,
                result,
                (string sql, StatementResult existing) => result
            );
        }

        public void AddOrUpdateExecutionTime(string method, string sql, ExecutionTime executionTime)
        {
            _executionTimes.AddOrUpdate(method + sql,
                executionTime,
                (string methodAndSql, ExecutionTime existing) => executionTime
            );
        }

        internal void AbortTransaction(TransactionId transactionId)
        {
            var prop = transactionId.GetType().GetProperty("Id", BindingFlags.Instance | BindingFlags.NonPublic);
            var id = (string) prop.GetValue(transactionId);
            _abortedTransactions.TryAdd(ByteString.FromBase64(id), true);
        }

        internal void AbortNextStatement()
        {
            lock (_lock)
            {
                _abortNextStatement = true;
            }
        }

        public IEnumerable<IMessage> Requests => new List<IMessage>(_requests).AsReadOnly();

        public void Reset()
        {
            _requests = new ConcurrentQueue<IMessage>();
            _executionTimes.Clear();
            _results.Clear();
        }

        public override Task<Transaction> BeginTransaction(BeginTransactionRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            TryFindSession(request.SessionAsSessionName);
            Transaction tx = new Transaction();
            var id = Interlocked.Increment(ref _transactionCounter);
            tx.Id = ByteString.CopyFromUtf8($"{request.SessionAsSessionName}/transactions/{id}");
            _transactions.TryAdd(tx.Id, tx);
            return Task.FromResult(tx);
        }

        public override Task<CommitResponse> Commit(CommitRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            TryFindSession(request.SessionAsSessionName);
            if (request.TransactionCase == CommitRequest.TransactionOneofCase.TransactionId)
            {
                TryFindTransaction(request.TransactionId, true);
            }
            CommitResponse response = new CommitResponse();
            Timestamp ts = Timestamp.FromDateTime(DateTime.UtcNow);
            response.CommitTimestamp = ts;
            return Task.FromResult(response);
        }

        private Session CreateSession(DatabaseName database)
        {
            var id = Interlocked.Increment(ref _sessionCounter);
            Session session = new Session { SessionName = new SessionName(database.ProjectId, database.InstanceId, database.DatabaseId, $"session-{id}") };
            if (!_sessions.TryAdd(session.SessionName, session))
            {
                throw new RpcException(new Grpc.Core.Status(StatusCode.AlreadyExists, $"Session with id session-{id} already exists"));
            }
            return session;
        }

        static internal RpcException CreateAbortedException(string message)
        {
            // Add a 100 nanosecond retry delay to the error to ensure that the delay is used, but does not slow
            // down the tests unnecessary (100ns == 1 Tick is the smallest possible measurable timespan in .NET).
            var key = RetryInfo.Descriptor.FullName + "-bin";
            var entry = new Grpc.Core.Metadata.Entry(key, new RetryInfo { RetryDelay = new Duration { Nanos = 100 } }.ToByteArray());
            var trailers = new Grpc.Core.Metadata { entry };

            var status = new Grpc.Core.Status(StatusCode.Aborted, $"Transaction aborted: {message}");
            var rpc = new RpcException(status, trailers);

            return rpc;
        }

        private Transaction TryFindTransaction(ByteString id, Boolean remove = false)
        {
            if (_abortedTransactions.TryGetValue(id, out bool aborted) && aborted)
            {
                throw CreateAbortedException("Transaction marked as aborted");
            }
            lock (_lock)
            {
                if (_abortNextStatement)
                {
                    _abortNextStatement = false;
                    throw CreateAbortedException("Next statement was aborted");
                }
            }
            if (remove ? _transactions.TryRemove(id, out Transaction tx) : _transactions.TryGetValue(id, out tx))
            {
                return tx;
            }
            throw new RpcException(new Grpc.Core.Status(StatusCode.NotFound, $"Transaction not found: {id.ToBase64()}"));
        }

        private Transaction FindOrBeginTransaction(SessionName session, TransactionSelector selector)
        {
            if (selector == null)
            {
                return BeginTransaction(session, SINGLE_USE, true);
            }
            // TODO: Check that the selected transaction actually belongs to the given session.
            return selector.SelectorCase switch
            {
                TransactionSelector.SelectorOneofCase.SingleUse => BeginTransaction(session, selector.SingleUse, true),
                TransactionSelector.SelectorOneofCase.Begin => BeginTransaction(session, selector.Begin, false),
                TransactionSelector.SelectorOneofCase.Id => TryFindTransaction(selector.Id),
                _ => null,
            };
        }

        private Transaction BeginTransaction(SessionName session, TransactionOptions options, bool singleUse)
        {
            Transaction tx = new Transaction();
            var id = Interlocked.Increment(ref _transactionCounter);
            tx.Id = ByteString.CopyFromUtf8($"{session}/transactions/{id}");
            if (options.ModeCase == TransactionOptions.ModeOneofCase.ReadOnly && options.ReadOnly.ReturnReadTimestamp)
            {
                tx.ReadTimestamp = Timestamp.FromDateTime(DateTime.Now);
            }
            if (!singleUse)
            {
                _transactions.TryAdd(tx.Id, tx);
                _transactionOptions.TryAdd(tx.Id, options);
            }
            return tx;
        }

        public override Task<BatchCreateSessionsResponse> BatchCreateSessions(BatchCreateSessionsRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            var database = request.DatabaseAsDatabaseName;
            BatchCreateSessionsResponse response = new BatchCreateSessionsResponse();
            for (int i = 0; i < request.SessionCount; i++)
            {
                response.Session.Add(CreateSession(database));
            }
            return Task.FromResult(response);
        }

        public override Task<Session> CreateSession(CreateSessionRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            var database = request.DatabaseAsDatabaseName;
            return Task.FromResult(CreateSession(database));
        }

        public override Task<Session> GetSession(GetSessionRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            return Task.FromResult(TryFindSession(request.SessionName));
        }

        public override Task<ListSessionsResponse> ListSessions(ListSessionsRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            ListSessionsResponse response = new ListSessionsResponse();
            foreach (Session session in _sessions.Values)
            {
                response.Sessions.Add(session);
            }
            return Task.FromResult(response);
        }

        public override Task<Empty> DeleteSession(DeleteSessionRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _sessions.TryRemove(request.SessionName, out _);
            return Task.FromResult(EMPTY);
        }

        private Session TryFindSession(SessionName name)
        {
            if (_sessions.TryGetValue(name, out Session session))
            {
                return session;
            }
            throw new RpcException(new Grpc.Core.Status(StatusCode.NotFound, $"Session not found: {name}"));
        }

        public override Task<ExecuteBatchDmlResponse> ExecuteBatchDml(ExecuteBatchDmlRequest request, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _ = TryFindSession(request.SessionAsSessionName);
            _ = FindOrBeginTransaction(request.SessionAsSessionName, request.Transaction);
            ExecuteBatchDmlResponse response = new ExecuteBatchDmlResponse
            {
                // TODO: Return other statuses based on the mocked results.
                Status = new Status()
            };
            response.Status.Code = (int)StatusCode.OK;
            int index = 0;
            foreach (var statement in request.Statements)
            {
                if (response.Status.Code != (int)StatusCode.OK)
                {
                    break;
                }
                if (_results.TryGetValue(statement.Sql, out StatementResult result))
                {
                    switch (result.Type)
                    {
                        case StatementResult.StatementResultType.RESULT_SET:
                            throw new RpcException(new Grpc.Core.Status(StatusCode.InvalidArgument, $"ResultSet is not a valid result type for BatchDml"));
                        case StatementResult.StatementResultType.UPDATE_COUNT:
                            response.ResultSets.Add(CreateUpdateCountResultSet(result.UpdateCount));
                            break;
                        case StatementResult.StatementResultType.EXCEPTION:
                            if (index == 0)
                            {
                                throw result.Exception;
                            }
                            response.Status = StatusFromException(result.Exception);
                            break;
                        default:
                            throw new RpcException(new Grpc.Core.Status(StatusCode.InvalidArgument, $"Invalid result type {result.Type} for {statement.Sql}"));
                    }
                }
                else
                {
                    throw new RpcException(new Grpc.Core.Status(StatusCode.InvalidArgument, $"No result found for {statement.Sql}"));
                }
                index++;
            }
            return Task.FromResult(response);
        }

        private Status StatusFromException(Exception e)
        {
            if (e is RpcException rpc)
            {
                return new Status { Code = (int)rpc.StatusCode, Message = e.Message };
            }
            return new Status { Code = (int)StatusCode.Unknown, Message = e.Message };
        }

        public override Task<ResultSet> ExecuteSql(ExecuteSqlRequest request, ServerCallContext context)
        {
            return base.ExecuteSql(request, context);
        }

        public override async Task ExecuteStreamingSql(ExecuteSqlRequest request, IServerStreamWriter<PartialResultSet> responseStream, ServerCallContext context)
        {
            _requests.Enqueue(request);
            _executionTimes.TryGetValue(nameof(ExecuteStreamingSql) + request.Sql, out ExecutionTime executionTime);
            Session session = TryFindSession(request.SessionAsSessionName);
            Transaction tx = FindOrBeginTransaction(request.SessionAsSessionName, request.Transaction);
            if (_results.TryGetValue(request.Sql, out StatementResult result))
            {
                switch (result.Type)
                {
                    case StatementResult.StatementResultType.RESULT_SET:
                        await WriteResultSet(result.ResultSet, responseStream, executionTime);
                        break;
                    case StatementResult.StatementResultType.UPDATE_COUNT:
                        await WriteUpdateCount(result.UpdateCount, responseStream);
                        break;
                    case StatementResult.StatementResultType.EXCEPTION:
                        throw result.Exception;
                    default:
                        throw new RpcException(new Grpc.Core.Status(StatusCode.InvalidArgument, $"Invalid result type {result.Type} for {request.Sql}"));
                }
            }
            else
            {
                throw new RpcException(new Grpc.Core.Status(StatusCode.InvalidArgument, $"No result found for {request.Sql}"));
            }
        }

        private async Task WriteResultSet(ResultSet resultSet, IServerStreamWriter<PartialResultSet> responseStream, ExecutionTime executionTime)
        {
            int index = 0;
            PartialResultSetsEnumerable enumerator = new PartialResultSetsEnumerable(resultSet);
            foreach (PartialResultSet prs in enumerator)
            {
                Exception e = executionTime?.PopExceptionAtIndex(index);
                if (e != null)
                {
                    throw e;
                }
                await responseStream.WriteAsync(prs);
                Thread.Sleep(100);
                index++;
            }
        }

        private async Task WriteUpdateCount(long updateCount, IServerStreamWriter<PartialResultSet> responseStream)
        {
            PartialResultSet prs = new PartialResultSet
            {
                Stats = new ResultSetStats { RowCountExact = updateCount }
            };
            await responseStream.WriteAsync(prs);
        }

        private ResultSet CreateUpdateCountResultSet(long updateCount)
        {
            ResultSet rs = new ResultSet
            {
                Stats = new ResultSetStats { RowCountExact = updateCount }
            };
            return rs;
        }

        public override Task<PartitionResponse> PartitionQuery(PartitionQueryRequest request, ServerCallContext context)
        {
            return base.PartitionQuery(request, context);
        }

        public override Task<PartitionResponse> PartitionRead(PartitionReadRequest request, ServerCallContext context)
        {
            return base.PartitionRead(request, context);
        }

        public override Task<ResultSet> Read(ReadRequest request, ServerCallContext context)
        {
            return base.Read(request, context);
        }

        public override Task<Empty> Rollback(RollbackRequest request, ServerCallContext context)
        {
            return base.Rollback(request, context);
        }

        public override Task StreamingRead(ReadRequest request, IServerStreamWriter<PartialResultSet> responseStream, ServerCallContext context)
        {
            return base.StreamingRead(request, responseStream, context);
        }
    }
}