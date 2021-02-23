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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests.Model;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Google.Cloud.EntityFrameworkCore.Spanner.IntegrationTests
{
    public class TransactionTests : IClassFixture<SpannerSampleFixture>
    {
        private readonly SpannerSampleFixture _fixture;

        public TransactionTests(SpannerSampleFixture fixture) => _fixture = fixture;

        [Fact]
        public async Task SaveChangesIsAtomic()
        {
            var singerId = _fixture.RandomLong();
            var invalidSingerId = _fixture.RandomLong();
            var albumId = _fixture.RandomLong();
            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Try to add a singer and an album in one transaction.
                // The album is invalid. Both the singer and the album
                // should not be inserted.
                db.Singers.Add(new Singers
                {
                    SingerId = singerId,
                    FirstName = "Joe",
                    LastName = "Elliot",
                });
                db.Albums.Add(new Albums
                {
                    AlbumId = albumId,
                    SingerId = invalidSingerId, // Invalid, does not reference an actual Singer
                    Title = "Some title",
                });
                await Assert.ThrowsAsync<SpannerBatchNonQueryException>(() => db.SaveChangesAsync());
            }

            using (var db = new TestSpannerSampleDbContext(_fixture.DatabaseName))
            {
                // Verify that the singer was not inserted in the database.
                Assert.Null(await db.Singers.FindAsync(singerId));
            }
        }

        [Fact]
        public async Task EndOfTransactionScopeCausesRollback()
        {
            var venueCode = _fixture.RandomString(4);
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            using (var transaction = await db.Database.BeginTransactionAsync())
            {
                db.Venues.AddRange(new Venues
                {
                    Code = venueCode,
                    Name = "Venue 3",
                });
                await db.SaveChangesAsync();
                // End the transaction scope without any explicit rollback.
            }
            // Verify that the venue was not inserted.
            var venuesAfterRollback = db.Venues
                .Where(v => v.Code == venueCode)
                .ToList();
            Assert.Empty(venuesAfterRollback);
        }

        [Fact]
        public async Task TransactionCanReadYourWrites()
        {
            var venueCode1 = _fixture.RandomString(4);
            var venueCode2 = _fixture.RandomString(4);
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);

            using var transaction = await db.Database.BeginTransactionAsync();
            // Add two venues in the transaction.
            db.Venues.AddRange(new Venues
            {
                Code = venueCode1,
                Name = "Venue 1",
            }, new Venues
            {
                Code = venueCode2,
                Name = "Venue 2",
            });
            await db.SaveChangesAsync();

            // Verify that we can read the venue while inside the transaction.
            var venues = db.Venues
                .Where(v => v.Code == venueCode1 || v.Code == venueCode2)
                .OrderBy(v => v.Name)
                .ToList();
            Assert.Equal(2, venues.Count);
            Assert.Equal("Venue 1", venues[0].Name);
            Assert.Equal("Venue 2", venues[1].Name);
            // Rollback and then verify that we should not be able to see the venues.
            await transaction.RollbackAsync();

            // Verify that the venues can no longer be read.
            var venuesAfterRollback = db.Venues
                .Where(v => v.Code == venueCode1 || v.Name == venueCode2)
                .ToList();
            Assert.Empty(venuesAfterRollback);
        }

        [Fact]
        public async Task TransactionCanReadCommitTimestamp()
        {
            var id = _fixture.RandomLong();
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);

            using var transaction = await db.Database.BeginTransactionAsync();
            // Add a row that will generate a commit timestamp.
            db.TableWithAllColumnTypes.Add(new TableWithAllColumnTypes { ColInt64 = id });
            await db.SaveChangesAsync();

            // Tables that have a pending commit timestamp cannot be read.
            // https://cloud.google.com/spanner/docs/commit-timestamp#dml
            // This also means that we cannot mark the commit timestamp column
            // as a column that has a generated value, as that would trigger a
            // result propagation during the same transaction.
            await Assert.ThrowsAsync<SpannerException>(() =>
                db.TableWithAllColumnTypes
                    .Where(r => r.ColInt64 == id)
                    .Select(r => new { r.ColInt64, r.ColCommitTs })
                    .FirstOrDefaultAsync());
            // Commit the transaction. This will generate a commit timestamp.
            await transaction.CommitAsync();

            // If we read the row back through the same database context using the primary key value,
            // we will get the cached object. This will not have a commit timestamp value, as EFCore
            // does not know that the column has a generated value.
            var rowUpdated = await db.TableWithAllColumnTypes.FindAsync(id);
            Assert.NotNull(rowUpdated);
            // TODO: Find a reasonable way to propagate the generated value to the current context.
            Assert.Null(rowUpdated.ColCommitTs);

            // Detaching the entity from the context and re-getting it will give us the most recent commit timestamp.
            db.Entry(rowUpdated).State = EntityState.Detached;
            if (SpannerFixtureBase.IsEmulator)
            {
                // The emulator does not support getting the enitire entity because of the ARRAY<NUMERIC> column.
                var rowRefreshed = await db.TableWithAllColumnTypes
                    .Where(r => r.ColInt64 == id)
                    .Select(r => new { r.ColInt64, r.ColCommitTs })
                    .FirstOrDefaultAsync();
                Assert.NotNull(rowRefreshed);
                Assert.NotNull(rowRefreshed.ColCommitTs);
            }
            else
            {
                var rowRefreshed = await db.TableWithAllColumnTypes.FindAsync(id);
                Assert.NotNull(rowRefreshed);
                Assert.NotNull(rowRefreshed.ColCommitTs);
            }
        }

        [Fact]
        public async Task CanUseSharedContextAndTransaction()
        {
            var venueCode = _fixture.RandomString(4);
            using var connection = _fixture.GetConnection();
            var options = new DbContextOptionsBuilder<SpannerSampleDbContext>()
                .UseSpanner(connection)
                .Options;
            using var context1 = new TestSpannerSampleDbContext(options);
            using var transaction = context1.Database.BeginTransaction();
            using (var context2 = new TestSpannerSampleDbContext(options))
            {
                await context2.Database.UseTransactionAsync(DbContextTransactionExtensions.GetDbTransaction(transaction));
                context2.Venues.Add(new Venues
                {
                    Code = venueCode,
                    Name = "Venue 3",
                });
                await context2.SaveChangesAsync();
            }
            // Check that the venue is readable from the other context.
            Assert.Equal("Venue 3", (await context1.Venues.FindAsync(venueCode)).Name);
            await transaction.CommitAsync();
            // Verify that it is also readable from a new unrelated context.
            using var context3 = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            Assert.Equal("Venue 3", (await context1.Venues.FindAsync(venueCode)).Name);
        }

        [Fact]
        public async Task CanUseReadOnlyTransaction()
        {
            var venueCode = _fixture.RandomString(4);
            var venueName = _fixture.RandomString(10);
            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            using var transaction = await db.Database.BeginTransactionAsync();

            // Add a venue.
            db.Venues.AddRange(new Venues
            {
                Code = venueCode,
                Name = venueName,
            });
            await db.SaveChangesAsync();
            await transaction.CommitAsync();
            var commitTimestamp = transaction.GetCommitTimestamp();

            // Try to read the venue using a read-only transaction.
            using var readOnlyTransaction = await db.Database.BeginReadOnlyTransactionAsync();
            var foundVenue = await db.Venues.Where(v => v.Code == venueCode).FirstOrDefaultAsync();
            Assert.NotNull(foundVenue);
            Assert.Equal(venueName, foundVenue.Name);
            // Read-only transactions cannot really be committed, but this releases the resources
            // that are used by the transaction and enables us to start a new transacton on the context.
            await readOnlyTransaction.CommitAsync();

            // Try to read the venue using a read-only transaction that reads using
            // a timestamp before the above venue was added. It should not return any results.
            using var readOnlyTransactionBeforeAdd = await db.Database.BeginReadOnlyTransactionAsync(TimestampBound.OfReadTimestamp(commitTimestamp.AddMilliseconds(-1)));
            var result = await db.Venues.Where(v => v.Code == venueCode).FirstOrDefaultAsync();
            Assert.Null(result);
        }

        [SkippableFact]
        public async Task CanUseComputedColumnAndCommitTimestamp()
        {
            Skip.If(SpannerFixtureBase.IsEmulator, "Emulator does not support inserting multiple rows in one table with a commit timestamp column in one transaction");
            var id1 = _fixture.RandomLong();
            var id2 = _fixture.RandomLong();

            using var db = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            db.TableWithAllColumnTypes.AddRange(
                new TableWithAllColumnTypes { ColInt64 = id1, ColStringArray = new List<string> { "1", "2", "3" } },
                new TableWithAllColumnTypes { ColInt64 = id2, ColStringArray = new List<string> { "4", "5", "6" } }
            );
            await db.SaveChangesAsync();
        }

        [SkippableTheory]
        [InlineData(false)]
        [InlineData(true)]
        public void TransactionRetry(bool disableInternalRetries)
        {
            Skip.If(SpannerFixtureBase.IsEmulator, "Emulator does not support multiple simultanous transactions");
            const int transactions = 8;
            var aborted = new List<Exception>();
            var res = Parallel.For(0, transactions, (i, state) =>
            {
                try
                {
                    // The internal retry mechanism should be able to catch and retry
                    // all aborted transactions. If internal retries are disabled, multiple
                    // transactions will abort.
                    InsertRandomSinger(disableInternalRetries).Wait();
                }
                catch (AggregateException e) when (e.InnerException is SpannerException se && se.ErrorCode == ErrorCode.Aborted)
                {
                    lock (aborted)
                    {
                        aborted.Add(se);
                    }
                    // We don't care exactly how many transactions were aborted, only whether
                    // at least one or none was aborted.
                    state.Stop();
                }
            });
            Assert.True(
                disableInternalRetries == (aborted.Count > 0),
                $"Unexpected aborted count {aborted.Count} for disableInternalRetries={disableInternalRetries}. First aborted error: {aborted.FirstOrDefault()?.Message ?? "<none>"}"
            );
        }

        private async Task InsertRandomSinger(bool disableInternalRetries)
        {
            var rnd = new Random(Guid.NewGuid().GetHashCode());
            using var context = new TestSpannerSampleDbContext(_fixture.DatabaseName);
            using var transaction = await context.Database.BeginTransactionAsync();
            if (disableInternalRetries)
            {
                transaction.DisableInternalRetries();
            }

            var rows = rnd.Next(1, 10);
            for (var row = 0; row < rows; row++)
            {
                // This test assumes that this is random enough and that the id's
                // will never overlap during a test run.
                var id = _fixture.RandomLong(rnd);
                var prefix = id.ToString("D20");
                // First name is required, so we just assign a meaningless random value.
                var firstName = "FirstName" + "-" + rnd.Next(10000).ToString("D4");
                // Last name contains the same value as the primary key with a random suffix.
                // This makes it possible to search for a singer using the last name and knowing
                // that the search will at most deliver one row (and it will be the same row each time).
                var lastName = prefix + "-" + rnd.Next(10000).ToString("D4");

                // Yes, this is highly inefficient, but that is intentional. This
                // will cause a large number of the transactions to be aborted.
                var existing = await context
                    .Singers
                    .Where(v => EF.Functions.Like(v.LastName, prefix + "%"))
                    .OrderBy(v => v.LastName)
                    .FirstOrDefaultAsync();

                if (existing == null)
                {
                    context.Singers.Add(new Singers
                    {
                        SingerId = id,
                        FirstName = firstName,
                        LastName = lastName,
                    });
                }
                else
                {
                    existing.FirstName = firstName;
                }
                await context.SaveChangesAsync();
            }
            await transaction.CommitAsync();
        }
    }
}
