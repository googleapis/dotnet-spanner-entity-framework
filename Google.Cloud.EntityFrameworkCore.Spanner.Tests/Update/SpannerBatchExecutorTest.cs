// Copyright 2026 Google LLC
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

using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal;
using Google.Cloud.Spanner.Data;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Xunit;

#pragma warning disable EF1001
namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests.Update
{
    public class SpannerBatchExecutorTest
    {
        private class MockDbException : DbException
        {
            public MockDbException(string message) : base(message)
            {
            }
        }

        [Fact]
        public void WrapException_PreservesNonStoreExceptions()
        {
            var invalidOperationException = new InvalidOperationException("No active transaction.");
            var argumentException = new ArgumentException("Invalid argument.");
            var operationCanceledException = new OperationCanceledException();
            var dbUpdateException = new DbUpdateException("Update failed.");

            var commands = Array.Empty<IReadOnlyModificationCommand>();

            Assert.Same(invalidOperationException, SpannerBatchExecutor.WrapException(invalidOperationException, commands));
            Assert.Same(argumentException, SpannerBatchExecutor.WrapException(argumentException, commands));
            Assert.Same(operationCanceledException, SpannerBatchExecutor.WrapException(operationCanceledException, commands));
            Assert.Same(dbUpdateException, SpannerBatchExecutor.WrapException(dbUpdateException, commands));
        }

        [Fact]
        public void WrapException_WrapsSpannerException()
        {
            var spannerException = new SpannerException(ErrorCode.AlreadyExists, "Row already exists");
            // Note: Google.Cloud.Spanner.Data.SpannerException inherits directly from System.Exception,
            // not System.Data.Common.DbException. Thus it must be handled explicitly.
            Assert.False(spannerException is DbException);
            var commands = Array.Empty<IReadOnlyModificationCommand>();

            var exception = SpannerBatchExecutor.WrapException(spannerException, commands);

            var dbUpdateException = Assert.IsType<DbUpdateException>(exception);
            Assert.Same(spannerException, dbUpdateException.InnerException);
            Assert.Empty(dbUpdateException.Entries);
        }

        [Fact]
        public void WrapException_WrapsRpcException()
        {
            var rpcException = new RpcException(new Status(StatusCode.AlreadyExists, "Already exists"));
            var commands = Array.Empty<IReadOnlyModificationCommand>();

            var exception = SpannerBatchExecutor.WrapException(rpcException, commands);

            var dbUpdateException = Assert.IsType<DbUpdateException>(exception);
            Assert.Same(rpcException, dbUpdateException.InnerException);
            Assert.Empty(dbUpdateException.Entries);
        }

        [Fact]
        public void WrapException_WrapsDbException()
        {
            var dbException = new MockDbException("Database error");
            var commands = Array.Empty<IReadOnlyModificationCommand>();

            var exception = SpannerBatchExecutor.WrapException(dbException, commands);

            var dbUpdateException = Assert.IsType<DbUpdateException>(exception);
            Assert.Same(dbException, dbUpdateException.InnerException);
            Assert.Empty(dbUpdateException.Entries);
        }

        private class FakeModificationCommand : IReadOnlyModificationCommand
        {
            public FakeModificationCommand(params IUpdateEntry[] entries)
            {
                Entries = entries;
            }

            public IReadOnlyList<IUpdateEntry> Entries { get; }
            public IReadOnlyList<IColumnModification> ColumnModifications => Array.Empty<IColumnModification>();
            public string TableName => "TestTable";
            public string Schema => null;
            public EntityState EntityState => EntityState.Modified;
            public ITable Table => null;
            public IStoreStoredProcedure StoreStoredProcedure => null;
            public IColumnBase RowsAffectedColumn => null;
            public void PropagateResults(RelationalDataReader reader) { }
            public void PropagateOutputParameters(DbParameterCollection parameterCollection, int index) { }
        }

        [Fact]
        public void CreateAggregateUpdateConcurrencyException_TruncatedUpdateCounts_OnlyIncludesUnexecutedEntries()
        {
            using var db = new MockServerSampleDbContext("Data Source=projects/p1/instances/i1/databases/d1");
            var singer1 = new Singers { SingerId = 1L, FirstName = "A", LastName = "B" };
            var singer2 = new Singers { SingerId = 2L, FirstName = "C", LastName = "D" };
            db.Attach(singer1);
            db.Attach(singer2);
            db.Entry(singer1).State = EntityState.Modified;
            db.Entry(singer2).State = EntityState.Modified;
            var entry1 = ((IInfrastructure<InternalEntityEntry>)db.Entry(singer1)).Instance;
            var entry2 = ((IInfrastructure<InternalEntityEntry>)db.Entry(singer2)).Instance;

            var command1 = new FakeModificationCommand(entry1);
            var command2 = new FakeModificationCommand(entry2);

            // Command 1 succeeded with count 1, but UpdateCounts was truncated so command 2 is unexecuted.
            var exception = SpannerModificationCommandBatch.CreateAggregateUpdateConcurrencyException(
                new IReadOnlyModificationCommand[] { command1, command2 },
                new long[] { 1L });

            var singleEntry = Assert.Single(exception.Entries);
            Assert.Same(singer2, singleEntry.Entity);
            Assert.DoesNotContain(exception.Entries, entry => ReferenceEquals(entry.Entity, singer1));
        }
    }
}
