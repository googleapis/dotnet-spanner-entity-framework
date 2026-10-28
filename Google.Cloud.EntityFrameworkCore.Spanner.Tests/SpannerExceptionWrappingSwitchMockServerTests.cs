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

using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal;
using Google.Cloud.Spanner.Data;
using Google.Cloud.Spanner.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore.Update;
using System;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable EF1001
namespace Google.Cloud.EntityFrameworkCore.Spanner.Tests
{
    [CollectionDefinition(nameof(DisableDbUpdateExceptionWrappingTestCollection), DisableParallelization = true)]
    public class DisableDbUpdateExceptionWrappingTestCollection
    {
    }

    /// <summary>
    /// Tests that verify the AppContext switch for disabling DbUpdateException wrapping.
    /// These tests execute in an isolated, non-parallel test collection to ensure that toggling
    /// the process-wide switch never interferes with other concurrent tests.
    /// </summary>
    [Collection(nameof(DisableDbUpdateExceptionWrappingTestCollection))]
    public class SpannerExceptionWrappingSwitchMockServerTests : IClassFixture<SpannerMockServerFixture>
    {
        private readonly SpannerMockServerFixture _fixture;

        public SpannerExceptionWrappingSwitchMockServerTests(SpannerMockServerFixture fixture)
        {
            _fixture = fixture;
            fixture.SpannerMock.Reset();
        }

        private string ConnectionString => $"Data Source=projects/p1/instances/i1/databases/d1;Host={_fixture.Host};Port={_fixture.Port}";

        [Fact]
        public void BatchDmlInsertFailsWithRawSpannerExceptionWhenSwitchEnabled()
        {
            AppContext.SetSwitch(SpannerDbContextOptionsBuilder.DisableDbUpdateExceptionWrappingSwitch, true);
            try
            {
                var insertSql = $"INSERT INTO `Venues` (`Code`, `Active`, `Capacity`, `Name`, `Ratings`)" +
                                $"{Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4)";
                _fixture.SpannerMock.AddOrUpdateStatementResult(
                    insertSql,
                    StatementResult.CreateException(new RpcException(new Status(StatusCode.AlreadyExists, "Row already exists in table Venues"))));

                using var db = new MockServerSampleDbContext(ConnectionString);
                using var transaction = db.Database.BeginTransaction();
                var venue = new Venues { Code = "V1", Name = "Venue 1" };
                db.Venues.Add(venue);

                var exception = Assert.Throws<SpannerException>(() => db.SaveChanges());
                Assert.Equal(ErrorCode.AlreadyExists, exception.ErrorCode);
            }
            finally
            {
                AppContext.SetSwitch(SpannerDbContextOptionsBuilder.DisableDbUpdateExceptionWrappingSwitch, false);
            }
        }

        [Fact]
        public async Task BatchDmlInsertFailsWithRawSpannerExceptionWhenSwitchEnabledAsync()
        {
            AppContext.SetSwitch(SpannerDbContextOptionsBuilder.DisableDbUpdateExceptionWrappingSwitch, true);
            try
            {
                var insertSql = $"INSERT INTO `Venues` (`Code`, `Active`, `Capacity`, `Name`, `Ratings`)" +
                                $"{Environment.NewLine}VALUES (@p0, @p1, @p2, @p3, @p4)";
                _fixture.SpannerMock.AddOrUpdateStatementResult(
                    insertSql,
                    StatementResult.CreateException(new RpcException(new Status(StatusCode.AlreadyExists, "Row already exists in table Venues"))));

                await using var db = new MockServerSampleDbContext(ConnectionString);
                await using var transaction = await db.Database.BeginTransactionAsync(TestContext.Current.CancellationToken);
                var venue = new Venues { Code = "V1", Name = "Venue 1" };
                db.Venues.Add(venue);

                var exception = await Assert.ThrowsAsync<SpannerException>(() => db.SaveChangesAsync(TestContext.Current.CancellationToken));
                Assert.Equal(ErrorCode.AlreadyExists, exception.ErrorCode);
            }
            finally
            {
                AppContext.SetSwitch(SpannerDbContextOptionsBuilder.DisableDbUpdateExceptionWrappingSwitch, false);
            }
        }

        [Fact]
        public void WrapException_ReturnsRawExceptionWhenSwitchEnabled()
        {
            AppContext.SetSwitch(SpannerBatchExecutor.DisableDbUpdateExceptionWrappingSwitch, true);
            try
            {
                var spannerException = new SpannerException(ErrorCode.AlreadyExists, "Row already exists");
                var commands = Array.Empty<IReadOnlyModificationCommand>();

                var exception = SpannerBatchExecutor.WrapException(spannerException, commands);

                Assert.Same(spannerException, exception);
            }
            finally
            {
                AppContext.SetSwitch(SpannerBatchExecutor.DisableDbUpdateExceptionWrappingSwitch, false);
            }
        }
    }
}
