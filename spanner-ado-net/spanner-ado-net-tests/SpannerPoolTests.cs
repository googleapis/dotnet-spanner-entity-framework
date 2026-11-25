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

using System.Collections.Concurrent;
using System.Data;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;
using Google.Cloud.SpannerLib.MockServer;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Status = Grpc.Core.Status;
using TypeCode = Google.Cloud.Spanner.V1.TypeCode;

namespace Google.Cloud.Spanner.DataProvider.Tests;

public class SpannerPoolCacheTests
{
    private static readonly TimeSpan DefaultTestTimeout = TimeSpan.FromSeconds(60);

    private static SpannerPoolCache CreatePoolCache(
        FakeSpannerLib fakeSpannerLib,
        FakeTimeProvider fakeTimeProvider,
        int maxIdlePools = 4,
        TimeSpan? idleTimeout = null)
    {
        return new SpannerPoolCache(
            fakeSpannerLib.CreatePool,
            maxIdlePools,
            idleTimeout ?? DefaultTestTimeout,
            fakeTimeProvider);
    }

    [Test]
    public void Acquire_SameConnectionString_ReusesPoolAndTracksActiveConnections()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var firstPool = poolCache.Acquire("dsn-1");
        var secondPool = poolCache.Acquire("dsn-1");

        Assert.That(secondPool, Is.SameAs(firstPool));
        Assert.That(fakeSpannerLib.CreatedConnectionStrings, Is.EqualTo(new[] { "dsn-1" }));
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));

        poolCache.Release(firstPool);
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        poolCache.Release(secondPool);
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);
    }

    [Test]
    public void EvictIdlePools_IdleTimeoutElapsed_ClosesUnusedPool()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var pool = poolCache.Acquire("dsn-1");
        poolCache.Release(pool);

        fakeTimeProvider.Advance(TimeSpan.FromSeconds(59));
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        fakeTimeProvider.Advance(TimeSpan.FromSeconds(1));
        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public void EvictionTimer_BeforeTimeoutElapsed_DoesNotEvict()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var pool = poolCache.Acquire("dsn-1");
        poolCache.Release(pool);

        fakeTimeProvider.Advance(TimeSpan.FromSeconds(30));
        fakeTimeProvider.FireTimersEarly();

        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);
    }

    [Test]
    public void EvictionTimer_FiresEarlyBeforeIdleTimeout_RearmsAndEvictsAtExpiry()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var pool = poolCache.Acquire("dsn-1");
        poolCache.Release(pool);

        // Simulate OS timer firing 5ms early before stopwatch elapsed reaches 60s.
        fakeTimeProvider.Advance(TimeSpan.FromSeconds(60) - TimeSpan.FromMilliseconds(5));
        fakeTimeProvider.FireTimersEarly();

        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        // Advancing the remaining 5ms must fire the re-armed timer and evict the pool.
        fakeTimeProvider.Advance(TimeSpan.FromMilliseconds(5));
        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public void EvictionTimer_FiresSubMillisecondEarly_RearmsToOneMillisecond()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var pool = poolCache.Acquire("dsn-1");
        poolCache.Release(pool);

        var halfMillisecond = TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond / 2);
        fakeTimeProvider.Advance(TimeSpan.FromSeconds(60) - halfMillisecond);
        fakeTimeProvider.FireTimersEarly();

        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        // Re-armed dueTime is clamped to 1ms so OS sub-millisecond jitter does not spin at 0ms.
        fakeTimeProvider.Advance(halfMillisecond);
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        fakeTimeProvider.Advance(halfMillisecond);
        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public void Acquire_ReusedBeforeIdleTimeout_ResetsIdleTimer()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var firstPool = poolCache.Acquire("dsn-1");
        poolCache.Release(firstPool);

        fakeTimeProvider.Advance(TimeSpan.FromSeconds(30));
        var secondPool = poolCache.Acquire("dsn-1");
        Assert.That(secondPool, Is.SameAs(firstPool));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));

        // Advancing past the original 60s mark must not close the pool while active.
        fakeTimeProvider.Advance(TimeSpan.FromSeconds(40));
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        poolCache.Release(secondPool);
        fakeTimeProvider.Advance(TimeSpan.FromSeconds(59));
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        fakeTimeProvider.Advance(TimeSpan.FromSeconds(1));
        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public void Acquire_NonHeadIdlePool_PreservesHeadTimerAndEvictsInOrder()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var pool1 = poolCache.Acquire("dsn-1");
        poolCache.Release(pool1);
        fakeTimeProvider.Advance(TimeSpan.FromSeconds(20));

        var pool2 = poolCache.Acquire("dsn-2");
        poolCache.Release(pool2);

        // Reactivating dsn-2 (non-head) must keep dsn-1's earlier expiry intact.
        var reusedPool2 = poolCache.Acquire("dsn-2");
        Assert.That(reusedPool2, Is.SameAs(pool2));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));

        fakeTimeProvider.Advance(TimeSpan.FromSeconds(40));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
        Assert.That(poolCache.Count, Is.EqualTo(1));

        poolCache.Release(reusedPool2);
        fakeTimeProvider.Advance(TimeSpan.FromSeconds(60));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L, 2L }));
        Assert.That(poolCache.Count, Is.EqualTo(0));
    }

    [Test]
    public void Release_ExceedsMaxIdlePools_EvictsLeastRecentlyUsedPool()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider, maxIdlePools: 2);

        var pool1 = poolCache.Acquire("dsn-1");
        poolCache.Release(pool1);
        fakeTimeProvider.Advance(TimeSpan.FromSeconds(1));

        var pool2 = poolCache.Acquire("dsn-2");
        poolCache.Release(pool2);
        fakeTimeProvider.Advance(TimeSpan.FromSeconds(1));

        // Touch dsn-1 so dsn-2 becomes the least-recently-used idle pool.
        var reusedPool1 = poolCache.Acquire("dsn-1");
        Assert.That(reusedPool1, Is.SameAs(pool1));
        poolCache.Release(reusedPool1);
        fakeTimeProvider.Advance(TimeSpan.FromSeconds(1));

        var pool3 = poolCache.Acquire("dsn-3");
        poolCache.Release(pool3);

        Assert.That(poolCache.Count, Is.EqualTo(2));
        Assert.That(poolCache.IdleCount, Is.EqualTo(2));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 2L }));

        // Verify dsn-1 and dsn-3 are still cached, while dsn-2 requires a new pool.
        var reacquired1 = poolCache.Acquire("dsn-1");
        var reacquired3 = poolCache.Acquire("dsn-3");
        var recreated2 = poolCache.Acquire("dsn-2");
        Assert.That(reacquired1, Is.SameAs(pool1));
        Assert.That(reacquired3, Is.SameAs(pool3));
        Assert.That(recreated2, Is.Not.SameAs(pool2));
        Assert.That(fakeSpannerLib.CreatedConnectionStrings, Is.EqualTo(new[] { "dsn-1", "dsn-2", "dsn-3", "dsn-2" }));

        poolCache.Release(reacquired1);
        poolCache.Release(reacquired3);
        poolCache.Release(recreated2);
    }

    [Test]
    public void Release_CalledTwiceOnSamePool_DoesNotDuplicateIdleEntryOrClosePrematurely()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider, maxIdlePools: 2);

        var pool1 = poolCache.Acquire("dsn-1");
        poolCache.Release(pool1);
        poolCache.Release(pool1);

        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        // Re-acquiring and acquiring a second pool must keep dsn-1 active even with maxIdlePools: 2.
        var activePool1 = poolCache.Acquire("dsn-1");
        var pool2 = poolCache.Acquire("dsn-2");
        var pool3 = poolCache.Acquire("dsn-3");
        poolCache.Release(pool2);
        poolCache.Release(pool3);

        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);
        Assert.That(poolCache.Count, Is.EqualTo(3));
        Assert.That(poolCache.IdleCount, Is.EqualTo(2));

        poolCache.Release(activePool1);
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 2L }));
    }

    [Test]
    public void Release_AfterDispose_IsNoOp()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var pool = poolCache.Acquire("dsn-1");
        poolCache.Dispose();
        poolCache.Release(pool);

        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public void Release_MaxIdlePoolsZero_ClosesImmediatelyOnRelease()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider, maxIdlePools: 0);

        var pool = poolCache.Acquire("dsn-1");
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        poolCache.Release(pool);
        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public void Release_ZeroIdleTimeout_ClosesImmediatelyOnRelease()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider, idleTimeout: TimeSpan.Zero);

        var pool = poolCache.Acquire("dsn-1");
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        poolCache.Release(pool);
        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public void Release_ActivePoolsExceedMaxIdlePools_NeverEvictsActivePools()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider, maxIdlePools: 1);

        var pool1 = poolCache.Acquire("dsn-1");
        var pool2 = poolCache.Acquire("dsn-2");
        var pool3 = poolCache.Acquire("dsn-3");

        Assert.That(poolCache.Count, Is.EqualTo(3));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        poolCache.Release(pool1);
        Assert.That(poolCache.Count, Is.EqualTo(3));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        poolCache.Release(pool2);
        Assert.That(poolCache.Count, Is.EqualTo(2));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));

        poolCache.Release(pool3);
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L, 2L }));
    }

    [Test]
    public void Acquire_PoolCreationFails_DoesNotCacheAndAllowsRetry()
    {
        var attempt = 0;
        var fakeSpannerLib = new FakeSpannerLib
        {
            OnCreatePool = connectionString =>
            {
                attempt++;
                if (attempt == 1)
                {
                    throw new InvalidOperationException($"Failed for {connectionString}");
                }
            },
        };
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        Assert.Throws<InvalidOperationException>(() => poolCache.Acquire("dsn-1"));
        Assert.That(poolCache.Count, Is.EqualTo(0));

        var pool = poolCache.Acquire("dsn-1");
        Assert.That(poolCache.Count, Is.EqualTo(1));
        poolCache.Release(pool);
    }

    [Test]
    public async Task Acquire_ConcurrentSameConnectionString_CreatesPoolOnce()
    {
        using var creationStarted = new ManualResetEventSlim(false);
        using var waiterEntered = new ManualResetEventSlim(false);
        using var allowFinish = new ManualResetEventSlim(false);
        var waitTimeout = TimeSpan.FromSeconds(5);
        var fakeSpannerLib = new FakeSpannerLib
        {
            OnCreatePool = _ =>
            {
                creationStarted.Set();
                Assert.That(allowFinish.Wait(waitTimeout), Is.True);
            },
        };
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);
        poolCache.OnWaitForEntry = () => waiterEntered.Set();

        var firstTask = Task.Run(() => poolCache.Acquire("dsn-1"));
        Assert.That(creationStarted.Wait(waitTimeout), Is.True);

        var secondTask = Task.Run(() => poolCache.Acquire("dsn-1"));
        Assert.That(waiterEntered.Wait(waitTimeout), Is.True);
        allowFinish.Set();

        var results = await Task.WhenAll(firstTask, secondTask);
        Assert.That(results[1], Is.SameAs(results[0]));
        Assert.That(fakeSpannerLib.CreatedConnectionStrings, Is.EqualTo(new[] { "dsn-1" }));
        Assert.That(results[0].ActiveConnections, Is.EqualTo(2));

        poolCache.Release(results[0]);
        poolCache.Release(results[1]);
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Acquire_ConcurrentSameConnectionStringFailure_PropagatesErrorAndAllowsRetry()
    {
        using var creationStarted = new ManualResetEventSlim(false);
        using var waiterEntered = new ManualResetEventSlim(false);
        using var allowFinish = new ManualResetEventSlim(false);
        var waitTimeout = TimeSpan.FromSeconds(5);
        var shouldFail = true;
        var fakeSpannerLib = new FakeSpannerLib
        {
            OnCreatePool = _ =>
            {
                if (shouldFail)
                {
                    creationStarted.Set();
                    Assert.That(allowFinish.Wait(waitTimeout), Is.True);
                    throw new InvalidOperationException("Create failed");
                }
            },
        };
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);
        poolCache.OnWaitForEntry = () => waiterEntered.Set();

        var firstTask = Task.Run(() => poolCache.Acquire("dsn-1"));
        Assert.That(creationStarted.Wait(waitTimeout), Is.True);

        var secondTask = Task.Run(() => poolCache.Acquire("dsn-1"));
        Assert.That(waiterEntered.Wait(waitTimeout), Is.True);
        allowFinish.Set();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await firstTask);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await secondTask);
        Assert.That(poolCache.Count, Is.EqualTo(0));

        shouldFail = false;
        var recoveredPool = poolCache.Acquire("dsn-1");
        Assert.That(poolCache.Count, Is.EqualTo(1));
        poolCache.Release(recoveredPool);
    }

    [Test]
    public async Task Acquire_ConcurrentDifferentConnectionStrings_DoesNotBlockOnSlowPoolCreation()
    {
        using var slowStarted = new ManualResetEventSlim(false);
        using var allowSlowFinish = new ManualResetEventSlim(false);
        var waitTimeout = TimeSpan.FromSeconds(5);
        var fakeSpannerLib = new FakeSpannerLib
        {
            OnCreatePool = connectionString =>
            {
                if (connectionString == "slow-dsn")
                {
                    slowStarted.Set();
                    Assert.That(allowSlowFinish.Wait(waitTimeout), Is.True);
                }
            },
        };
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var slowTask = Task.Run(() => poolCache.Acquire("slow-dsn"));
        Assert.That(slowStarted.Wait(waitTimeout), Is.True);

        var fastPool = poolCache.Acquire("fast-dsn");
        Assert.That(slowTask.IsCompleted, Is.False);

        allowSlowFinish.Set();
        var slowPool = await slowTask;

        Assert.That(poolCache.Count, Is.EqualTo(2));
        poolCache.Release(fastPool);
        poolCache.Release(slowPool);
    }

    [Test]
    public void ConstructorAndAcquire_InvalidArguments_ThrowExpectedExceptions()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();

        Assert.Throws<ArgumentNullException>(() => _ = new SpannerPoolCache(null!, 4, DefaultTestTimeout, fakeTimeProvider));
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = new SpannerPoolCache(fakeSpannerLib.CreatePool, -1, DefaultTestTimeout, fakeTimeProvider));
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = new SpannerPoolCache(fakeSpannerLib.CreatePool, 4, TimeSpan.FromSeconds(-1), fakeTimeProvider));
        Assert.Throws<ArgumentNullException>(() => _ = new SpannerPoolCache(fakeSpannerLib.CreatePool, 4, DefaultTestTimeout, null!));

        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);
        Assert.Throws<ArgumentNullException>(() => poolCache.Acquire(null!));
    }

    [Test]
    public void Release_IdleTimeoutExceedsMaxTimerDueTime_RearmsAndEvictsAtExpiry()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        var idleTimeout = TimeSpan.FromDays(60);
        var maxTimerDueTime = TimeSpan.FromMilliseconds(uint.MaxValue - 1);
        using var poolCache = CreatePoolCache(
            fakeSpannerLib,
            fakeTimeProvider,
            idleTimeout: idleTimeout);

        var pool = poolCache.Acquire("dsn-1");
        poolCache.Release(pool);

        // First timer hop clamps at MaxTimerDueTime (~49.7 days) without evicting prematurely.
        fakeTimeProvider.Advance(maxTimerDueTime);
        Assert.That(poolCache.Count, Is.EqualTo(1));
        Assert.That(poolCache.IdleCount, Is.EqualTo(1));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.Empty);

        // Advancing the remaining duration fires the re-armed timer and evicts the pool at 60 days.
        fakeTimeProvider.Advance(idleTimeout - maxTimerDueTime);
        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public async Task Dispose_WhileCreationAndWaiterInFlight_BothThrowAndCloseOrphanPool()
    {
        using var started = new ManualResetEventSlim(false);
        using var waiterEntered = new ManualResetEventSlim(false);
        using var allowFinish = new ManualResetEventSlim(false);
        var waitTimeout = TimeSpan.FromSeconds(5);
        var fakeSpannerLib = new FakeSpannerLib
        {
            OnCreatePool = _ =>
            {
                started.Set();
                Assert.That(allowFinish.Wait(waitTimeout), Is.True);
            },
        };
        var fakeTimeProvider = new FakeTimeProvider();
        var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);
        poolCache.OnWaitForEntry = () => waiterEntered.Set();

        var firstAcquire = Task.Run(() => poolCache.Acquire("dsn-1"));
        Assert.That(started.Wait(waitTimeout), Is.True);

        var secondAcquire = Task.Run(() => poolCache.Acquire("dsn-1"));
        Assert.That(waiterEntered.Wait(waitTimeout), Is.True);

        poolCache.Dispose();
        allowFinish.Set();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await firstAcquire);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await secondAcquire);
        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public void Dispose_ThenOperations_BehavesSafely()
    {
        var fakeSpannerLib = new FakeSpannerLib();
        var fakeTimeProvider = new FakeTimeProvider();
        var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var pool = poolCache.Acquire("dsn-1");
        poolCache.Release(pool);

        poolCache.Dispose();
        poolCache.Dispose();

        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L }));
        Assert.Throws<ObjectDisposedException>(() => poolCache.Acquire("dsn-1"));
    }

    [Test]
    public void EvictIdlePools_ClosePoolThrows_ContinuesEvictingRemainingPools()
    {
        var fakeSpannerLib = new FakeSpannerLib
        {
            OnClosePool = pool =>
            {
                if (pool.Id == 1L)
                {
                    throw new InvalidOperationException("Close failed");
                }
            },
        };
        var fakeTimeProvider = new FakeTimeProvider();
        using var poolCache = CreatePoolCache(fakeSpannerLib, fakeTimeProvider);

        var pool1 = poolCache.Acquire("dsn-1");
        var pool2 = poolCache.Acquire("dsn-2");
        poolCache.Release(pool1);
        poolCache.Release(pool2);

        fakeTimeProvider.Advance(TimeSpan.FromSeconds(60));

        Assert.That(poolCache.Count, Is.EqualTo(0));
        Assert.That(poolCache.IdleCount, Is.EqualTo(0));
        Assert.That(fakeSpannerLib.ClosedPoolIds, Is.EqualTo(new[] { 1L, 2L }));
    }

    private sealed class FakeSpannerLib : ISpannerLib
    {
        private long _nextPoolId;
        public List<string> CreatedConnectionStrings { get; } = [];
        public ConcurrentQueue<long> ClosedPoolIds { get; } = new();
        public Action<string>? OnCreatePool { get; init; }
        public Action<Pool>? OnClosePool { get; init; }

        public Pool CreatePool(string connectionString)
        {
            OnCreatePool?.Invoke(connectionString);
            lock (CreatedConnectionStrings)
            {
                CreatedConnectionStrings.Add(connectionString);
            }
            var id = Interlocked.Increment(ref _nextPoolId);
            return new Pool(this, id);
        }

        public void ClosePool(Pool pool)
        {
            ClosedPoolIds.Enqueue(pool.Id);
            OnClosePool?.Invoke(pool);
        }

        public Connection CreateConnection(Pool pool) => throw new NotImplementedException();
        public void CloseConnection(Connection connection) => throw new NotImplementedException();
        public CommitResponse? WriteMutations(Connection connection, BatchWriteRequest.Types.MutationGroup mutations) => throw new NotImplementedException();
        public Task<CommitResponse?> WriteMutationsAsync(Connection connection, BatchWriteRequest.Types.MutationGroup mutations, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Rows Execute(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0) => throw new NotImplementedException();
        public Task<Rows> ExecuteAsync(Connection connection, ExecuteSqlRequest statement, int prefetchRows = 0, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public long[] ExecuteBatch(Connection connection, ExecuteBatchDmlRequest statements) => throw new NotImplementedException();
        public Task<long[]> ExecuteBatchAsync(Connection connection, ExecuteBatchDmlRequest statements, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public ResultSetMetadata? Metadata(Rows rows) => throw new NotImplementedException();
        public Task<ResultSetMetadata?> MetadataAsync(Rows rows, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public ResultSetMetadata? NextResultSet(Rows rows) => throw new NotImplementedException();
        public Task<ResultSetMetadata?> NextResultSetAsync(Rows rows, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public ResultSetStats? Stats(Rows rows) => throw new NotImplementedException();
        public ListValue? Next(Rows rows, int numRows, ISpannerLib.RowEncoding encoding) => throw new NotImplementedException();
        public Task<ListValue?> NextAsync(Rows rows, int numRows, ISpannerLib.RowEncoding encoding, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public void CloseRows(Rows rows) => throw new NotImplementedException();
        public void BeginTransaction(Connection connection, TransactionOptions transactionOptions) => throw new NotImplementedException();
        public Task BeginTransactionAsync(Connection connection, TransactionOptions transactionOptions, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public CommitResponse? Commit(Connection connection) => throw new NotImplementedException();
        public Task<CommitResponse?> CommitAsync(Connection connection, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public void Rollback(Connection connection) => throw new NotImplementedException();
        public Task RollbackAsync(Connection connection, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    }

    private sealed class FakeTimeProvider : TimeProvider
    {
        private readonly object _lock = new();
        private readonly List<FakeTimer> _timers = [];
        private long _utcNowTicks = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp()
        {
            lock (_lock)
            {
                return _utcNowTicks;
            }
        }

        public override DateTimeOffset GetUtcNow()
        {
            lock (_lock)
            {
                return new DateTimeOffset(_utcNowTicks, TimeSpan.Zero);
            }
        }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new FakeTimer(this, callback, state);
            lock (_lock)
            {
                _timers.Add(timer);
                timer.UpdateLocked(dueTime, period, _utcNowTicks);
            }
            return timer;
        }

        public void FireTimersEarly()
        {
            List<FakeTimer> activeTimers;
            lock (_lock)
            {
                activeTimers = [];
                foreach (var timer in _timers)
                {
                    if (!timer.Disposed && timer.DueTicks != long.MaxValue)
                    {
                        timer.DueTicks = long.MaxValue;
                        activeTimers.Add(timer);
                    }
                }
            }
            foreach (var timer in activeTimers)
            {
                timer.Fire();
            }
        }

        public void Advance(TimeSpan delta)
        {
            long targetTicks;
            lock (_lock)
            {
                targetTicks = _utcNowTicks + delta.Ticks;
                _utcNowTicks = targetTicks;
            }

            while (true)
            {
                FakeTimer? dueTimer = null;
                lock (_lock)
                {
                    foreach (var timer in _timers)
                    {
                        if (!timer.Disposed && timer.DueTicks != long.MaxValue && timer.DueTicks <= targetTicks)
                        {
                            if (dueTimer == null || timer.DueTicks < dueTimer.DueTicks)
                            {
                                dueTimer = timer;
                            }
                        }
                    }

                    if (dueTimer == null)
                    {
                        break;
                    }

                    if (dueTimer.PeriodTicks > 0)
                    {
                        dueTimer.DueTicks += dueTimer.PeriodTicks;
                    }
                    else
                    {
                        dueTimer.DueTicks = long.MaxValue;
                    }
                }

                dueTimer.Fire();
            }
        }

        private sealed class FakeTimer(FakeTimeProvider timeProvider, TimerCallback callback, object? state) : ITimer
        {
            public long DueTicks { get; set; } = long.MaxValue;
            public long PeriodTicks { get; private set; } = Timeout.Infinite;
            public bool Disposed { get; private set; }

            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                lock (timeProvider._lock)
                {
                    if (Disposed)
                    {
                        return false;
                    }
                    UpdateLocked(dueTime, period, timeProvider._utcNowTicks);
                    return true;
                }
            }

            public void UpdateLocked(TimeSpan dueTime, TimeSpan period, long nowTicks)
            {
                DueTicks = dueTime == Timeout.InfiniteTimeSpan ? long.MaxValue : nowTicks + dueTime.Ticks;
                PeriodTicks = period == Timeout.InfiniteTimeSpan ? -1 : period.Ticks;
            }

            public void Fire() => callback(state);

            public void Dispose()
            {
                lock (timeProvider._lock)
                {
                    Disposed = true;
                    DueTicks = long.MaxValue;
                    timeProvider._timers.Remove(this);
                }
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }
}

public class SpannerPoolTests : AbstractMockServerTests
{
    private string ConnectionStringForDatabase(string database) =>
        $"Host={Fixture.Host};Port={Fixture.Port};Data Source=projects/p1/instances/i1/databases/{database};UsePlainText=true";

    private static void WithMaxIdlePools(string? maxIdlePools, Action action)
    {
        var previousMaxIdlePools = Environment.GetEnvironmentVariable("SPANNER_ADO_MAX_IDLE_POOLS");
        Environment.SetEnvironmentVariable("SPANNER_ADO_MAX_IDLE_POOLS", maxIdlePools);
        SpannerPool.CloseSpannerLib();
        try
        {
            action();
        }
        finally
        {
            Environment.SetEnvironmentVariable("SPANNER_ADO_MAX_IDLE_POOLS", previousMaxIdlePools);
            SpannerPool.CloseSpannerLib();
        }
    }

    [Test]
    [NonParallelizable]
    public void CloseSpannerLib_WhenUnused_DoesNotThrowOrCreatePools()
    {
        SpannerPool.CloseSpannerLib();
        Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(0));
        Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(0));
    }

    [Test]
    [NonParallelizable]
    public void SpannerConnection_MaxIdlePoolsEnvironmentVariable_BoundsCachedIdlePools()
    {
        WithMaxIdlePools("2", () =>
        {
            for (var index = 1; index <= 4; index++)
            {
                using var connection = new SpannerConnection(ConnectionStringForDatabase($"d{index}"));
                connection.Open();
                Assert.That(connection.ExecuteScalar("SELECT 1"), Is.EqualTo(1L));
                connection.Close();
            }

            Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(2));
            Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(2));
        });
    }

    [Test]
    [NonParallelizable]
    public void SpannerConnection_ConcurrentActiveExceedingMaxIdlePools_KeepsActiveAndReopensEvicted()
    {
        WithMaxIdlePools("1", () =>
        {
            using var firstConnection = new SpannerConnection(ConnectionStringForDatabase("d1"));
            using var secondConnection = new SpannerConnection(ConnectionStringForDatabase("d2"));

            firstConnection.Open();
            secondConnection.Open();
            Assert.That(firstConnection.ExecuteScalar("SELECT 1"), Is.EqualTo(1L));
            Assert.That(secondConnection.ExecuteScalar("SELECT 1"), Is.EqualTo(1L));
            Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(2));
            Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(0));

            firstConnection.Close();
            Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(2));
            Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(1));

            secondConnection.Close();
            Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(1));
            Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(1));

            // Re-opening the evicted first database succeeds cleanly.
            using var reopenedConnection = new SpannerConnection(ConnectionStringForDatabase("d1"));
            reopenedConnection.Open();
            Assert.That(reopenedConnection.ExecuteScalar("SELECT 1"), Is.EqualTo(1L));
            Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(2));
            Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(1));
        });
    }

    [Test]
    [NonParallelizable]
    public void SpannerConnection_ReopenAndFailedOpen_MaintainsAccuratePoolReferenceCount()
    {
        WithMaxIdlePools("1", () =>
        {
            const string detectDialectQuery =
                "select option_value from information_schema.database_options where option_name='database_dialect'";
            Fixture.SpannerMock.AddOrUpdateStatementResult(
                detectDialectQuery,
                StatementResult.CreateException(new RpcException(new Status(StatusCode.NotFound, "Database not found"))));

            using (var failingConnection = new SpannerConnection(ConnectionString))
            {
                Assert.Throws<SpannerException>(() => failingConnection.Open());
                Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(0));
            }

            Fixture.SpannerMock.AddOrUpdateStatementResult(
                detectDialectQuery,
                StatementResult.CreateResultSet(
                    [Tuple.Create(TypeCode.String, "option_value")],
                    [["GOOGLE_STANDARD_SQL"]]));

            using (var connection = new SpannerConnection(ConnectionString))
            {
                connection.Open();
                Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(1));
                Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(0));

                connection.Close();
                Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(1));
                Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(1));

                connection.Open();
                Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(1));
                Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(0));
            }

            Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(1));
            Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(1));
        });
    }

    [Test]
    [NonParallelizable]
    public void SpannerConnection_OpenThrowsInStateChange_ReleasesPoolAndRecovers()
    {
        WithMaxIdlePools("1", () =>
        {
            using var connection = new SpannerConnection(ConnectionString);
            var shouldThrow = true;
            connection.StateChange += (_, eventArgs) =>
            {
                if (shouldThrow && eventArgs.CurrentState == ConnectionState.Open)
                {
                    shouldThrow = false;
                    throw new InvalidOperationException("StateChange failure");
                }
            };

            Assert.Throws<InvalidOperationException>(() => connection.Open());
            Assert.That(connection.State, Is.EqualTo(ConnectionState.Closed));
            Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(1));
            Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(1));

            connection.Open();
            Assert.That(connection.State, Is.EqualTo(ConnectionState.Open));
            Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(1));
            Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(0));
            Assert.That(connection.ExecuteScalar("SELECT 1"), Is.EqualTo(1L));
        });
    }

    [Test]
    [NonParallelizable]
    public void SpannerConnection_OpenStateChangeClosesConnectionThenThrows_DoesNotDoubleRelease()
    {
        WithMaxIdlePools("0", () =>
        {
            using var otherConnection = new SpannerConnection(ConnectionString);
            using var connection = new SpannerConnection(ConnectionString);
            var closedStateChangeCount = 0;
            connection.StateChange += (_, eventArgs) =>
            {
                if (eventArgs.CurrentState == ConnectionState.Closed)
                {
                    closedStateChangeCount++;
                }
                else if (eventArgs.CurrentState == ConnectionState.Open)
                {
                    connection.Close();
                    otherConnection.Open();
                    throw new InvalidOperationException("StateChange failure after close");
                }
            };

            Assert.Throws<InvalidOperationException>(() => connection.Open());
            Assert.That(connection.State, Is.EqualTo(ConnectionState.Closed));
            Assert.That(closedStateChangeCount, Is.EqualTo(1));
            // otherConnection must remain open and the shared pool must not have been released/evicted.
            Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(1));
            Assert.That(SpannerPool.IdlePoolCount, Is.EqualTo(0));
            Assert.That(otherConnection.ExecuteScalar("SELECT 1"), Is.EqualTo(1L));
        });
    }

    [Test]
    [NonParallelizable]
    public void MaxIdlePoolsFromEnvironment_InvalidValues_ThrowsWithoutPoisoningSpannerPool()
    {
        WithMaxIdlePools("-1", () =>
        {
            using var connection = new SpannerConnection(ConnectionString);
            Assert.Throws<ArgumentOutOfRangeException>(() => connection.Open());
        });

        WithMaxIdlePools("not-a-number", () =>
        {
            using var connection = new SpannerConnection(ConnectionString);
            Assert.Throws<FormatException>(() => connection.Open());
        });

        WithMaxIdlePools("2", () =>
        {
            using var connection = new SpannerConnection(ConnectionString);
            connection.Open();
            Assert.That(connection.ExecuteScalar("SELECT 1"), Is.EqualTo(1L));
            Assert.That(SpannerPool.CachedPoolCount, Is.EqualTo(1));
        });
    }
}
