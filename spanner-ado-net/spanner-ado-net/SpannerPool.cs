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
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.SpannerLib;
using Google.Cloud.SpannerLib.Grpc;

namespace Google.Cloud.Spanner.DataProvider;

internal class SpannerPool
{
    internal const int DefaultMaxIdlePools = 128;
    internal static readonly TimeSpan DefaultIdleTimeout = TimeSpan.FromSeconds(60);
    private const string MaxIdlePoolsEnvironmentVariable = "SPANNER_ADO_MAX_IDLE_POOLS";

    private static bool UseNativeLib => bool.Parse(Environment.GetEnvironmentVariable("SPANNER_ADO_USE_NATIVE_LIB") ?? "false");
    
    private static int NumChannels => int.Parse(Environment.GetEnvironmentVariable("SPANNER_ADO_NUM_CHANNELS") ?? "4");
    
    private static GrpcLibSpanner.CommunicationStyle CommunicationStyle => Enum.Parse<GrpcLibSpanner.CommunicationStyle>(Environment.GetEnvironmentVariable("SPANNER_ADO_COMMUNICATION_STYLE") ?? nameof(GrpcLibSpanner.CommunicationStyle.BidiStreaming));

    internal static int MaxIdlePoolsFromEnvironment
    {
        get
        {
            var value = Environment.GetEnvironmentVariable(MaxIdlePoolsEnvironmentVariable);
            if (string.IsNullOrWhiteSpace(value))
            {
                return DefaultMaxIdlePools;
            }
            var maxIdlePools = int.Parse(value, CultureInfo.InvariantCulture);
            if (maxIdlePools < 0)
            {
                throw new ArgumentOutOfRangeException(
                    MaxIdlePoolsEnvironmentVariable,
                    maxIdlePools,
                    $"{MaxIdlePoolsEnvironmentVariable} must be non-negative.");
            }
            return maxIdlePools;
        }
    }

    private static readonly object GrpcLibLock = new();
    private static ISpannerLib? _gRpcSpannerLib;

    private static ISpannerLib GetOrCreateGrpcSpannerLib()
    {
        lock (GrpcLibLock)
        {
            _gRpcSpannerLib ??= new GrpcLibSpanner(numChannels: NumChannels, communicationStyle: CommunicationStyle);
            return _gRpcSpannerLib;
        }
    }

    private static readonly object DefaultCacheLock = new();
    private static SpannerPoolCache? _defaultCache;

    private static SpannerPoolCache GetOrCreateDefaultCache()
    {
        lock (DefaultCacheLock)
        {
            if (_defaultCache == null)
            {
                SpannerPoolCache? createdCache = null;
                createdCache = new SpannerPoolCache(
                    connectionString => CreateLibPool(createdCache!, connectionString),
                    MaxIdlePoolsFromEnvironment,
                    DefaultIdleTimeout,
                    TimeProvider.System);
                _defaultCache = createdCache;
            }
            return _defaultCache;
        }
    }

    private static Pool CreateLibPool(SpannerPoolCache expectedCache, string connectionString)
    {
        ISpannerLib spannerLib;
        lock (DefaultCacheLock)
        {
            if (!ReferenceEquals(_defaultCache, expectedCache))
            {
                throw new ObjectDisposedException(nameof(SpannerPool));
            }
            spannerLib = GetOrCreateGrpcSpannerLib();
        }
        return Pool.Create(spannerLib, connectionString);
    }

    internal static int CachedPoolCount
    {
        get
        {
            lock (DefaultCacheLock)
            {
                return _defaultCache?.Count ?? 0;
            }
        }
    }

    internal static int IdlePoolCount
    {
        get
        {
            lock (DefaultCacheLock)
            {
                return _defaultCache?.IdleCount ?? 0;
            }
        }
    }

    internal static SpannerPool Acquire(string connectionString) => GetOrCreateDefaultCache().Acquire(connectionString);

    internal static void CloseSpannerLib()
    {
        lock (DefaultCacheLock)
        {
            var cache = _defaultCache;
            _defaultCache = null;
            cache?.Dispose();
            lock (GrpcLibLock)
            {
                _gRpcSpannerLib?.Dispose();
                _gRpcSpannerLib = null;
            }
        }
    }

    private readonly SpannerPoolCache _cache;
    private readonly Pool _libPool;

    internal string ConnectionString { get; }
    internal int ActiveConnections { get; set; }
    internal LinkedListNode<SpannerPool>? IdleNode { get; set; }
    internal long IdleSinceTicks { get; set; }
    internal bool Closed { get; set; }

    internal SpannerPool(SpannerPoolCache cache, string connectionString, Pool libPool)
    {
        _cache = cache;
        ConnectionString = connectionString;
        _libPool = libPool;
    }

    internal void Release() => _cache.Release(this);

    internal void CloseLibPoolSafely() => CloseLibPoolSafely(_libPool);

    internal static void CloseLibPoolSafely(Pool libPool)
    {
        try
        {
            libPool.Close();
        }
        catch (Exception)
        {
            // Best-effort cleanup of evicted or invalidated pools.
        }
    }

    internal Connection CreateConnection()
    {
        return _libPool.CreateConnection();
    }
}

internal sealed class SpannerPoolCache : IDisposable
{
    private static readonly TimeSpan MinTimerDueTime = TimeSpan.FromMilliseconds(1);
    private static readonly TimeSpan MaxTimerDueTime = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private sealed class CacheEntry
    {
        public TaskCompletionSource ReadySource { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public SpannerPool? Pool { get; set; }
        public Exception? Error { get; set; }
    }

    private readonly object _lock = new();
    private readonly Func<string, Pool> _createPool;
    private readonly TimeSpan _idleTimeout;
    private readonly TimeProvider _timeProvider;
    private readonly ITimer _evictionTimer;
    private readonly Dictionary<string, CacheEntry> _entries = new(StringComparer.Ordinal);
    private readonly LinkedList<SpannerPool> _idlePools = new();

    private readonly int _maxIdlePools;
    private bool _timerArmed;
    private long _timerHeadIdleTicks;
    private bool _disposed;

    internal Action? OnWaitForEntry { get; set; }

    internal SpannerPoolCache(
        Func<string, Pool> createPool,
        int maxIdlePools,
        TimeSpan idleTimeout,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(createPool);
        ArgumentOutOfRangeException.ThrowIfNegative(maxIdlePools);
        ArgumentOutOfRangeException.ThrowIfLessThan(idleTimeout, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(timeProvider);

        _createPool = createPool;
        _maxIdlePools = maxIdlePools;
        _idleTimeout = idleTimeout;
        _timeProvider = timeProvider;
        _evictionTimer = _timeProvider.CreateTimer(
            static state => ((SpannerPoolCache)state!).OnEvictionTimer(),
            this,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);
    }

    internal int Count
    {
        get
        {
            lock (_lock)
            {
                return _entries.Count;
            }
        }
    }

    internal int IdleCount
    {
        get
        {
            lock (_lock)
            {
                return _idlePools.Count;
            }
        }
    }

    internal SpannerPool Acquire(string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        while (true)
        {
            if (TryAcquireExisting(connectionString, out var readyPool, out var inFlightEntry, out var reservedEntry))
            {
                return readyPool!;
            }

            if (inFlightEntry != null)
            {
                WaitForEntry(inFlightEntry);
                continue;
            }

            return CreateAndRegisterPool(connectionString, reservedEntry!);
        }
    }

    private bool TryAcquireExisting(
        string connectionString,
        out SpannerPool? readyPool,
        out CacheEntry? inFlightEntry,
        out CacheEntry? reservedEntry)
    {
        readyPool = null;
        inFlightEntry = null;
        reservedEntry = null;

        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_entries.TryGetValue(connectionString, out var existingEntry))
            {
                if (existingEntry.Pool != null)
                {
                    readyPool = ActivatePoolLocked(existingEntry.Pool);
                    return true;
                }
                inFlightEntry = existingEntry;
                return false;
            }

            reservedEntry = new CacheEntry();
            _entries[connectionString] = reservedEntry;
            return false;
        }
    }

    private SpannerPool ActivatePoolLocked(SpannerPool pool)
    {
        pool.ActiveConnections++;
        if (pool.IdleNode != null)
        {
            _idlePools.Remove(pool.IdleNode);
            pool.IdleNode = null;
            pool.IdleSinceTicks = 0;
            UpdateTimerLocked();
        }
        return pool;
    }

    private void WaitForEntry(CacheEntry entry)
    {
        OnWaitForEntry?.Invoke();
        entry.ReadySource.Task.GetAwaiter().GetResult();
        if (entry.Error != null)
        {
            ExceptionDispatchInfo.Capture(entry.Error).Throw();
        }
    }

    private SpannerPool CreateAndRegisterPool(string connectionString, CacheEntry entry)
    {
        Pool libPool;
        try
        {
            libPool = _createPool(connectionString);
        }
        catch (Exception exception)
        {
            lock (_lock)
            {
                FailEntryLocked(connectionString, entry, exception);
            }
            throw;
        }

        SpannerPool? createdPool = null;
        ObjectDisposedException? invalidatedError = null;
        lock (_lock)
        {
            if (_disposed)
            {
                invalidatedError = new ObjectDisposedException(nameof(SpannerPool));
                FailEntryLocked(connectionString, entry, invalidatedError);
            }
            else
            {
                createdPool = new SpannerPool(this, connectionString, libPool)
                {
                    ActiveConnections = 1,
                };
                entry.Pool = createdPool;
                entry.ReadySource.TrySetResult();
            }
        }

        if (invalidatedError != null)
        {
            SpannerPool.CloseLibPoolSafely(libPool);
            throw invalidatedError;
        }

        return createdPool!;
    }

    private void FailEntryLocked(string connectionString, CacheEntry entry, Exception error)
    {
        if (_entries.TryGetValue(connectionString, out var current) && ReferenceEquals(current, entry))
        {
            _entries.Remove(connectionString);
        }
        entry.Error = error;
        entry.ReadySource.TrySetResult();
    }

    internal void Release(SpannerPool pool)
    {
        List<SpannerPool>? poolsToClose;
        lock (_lock)
        {
            if (_disposed || pool.Closed || pool.ActiveConnections <= 0 || pool.IdleNode != null)
            {
                return;
            }
            pool.ActiveConnections--;
            if (pool.ActiveConnections > 0)
            {
                return;
            }

            pool.IdleSinceTicks = _timeProvider.GetTimestamp();
            pool.IdleNode = _idlePools.AddLast(pool);

            poolsToClose = CollectPoolsToEvictLocked();
            UpdateTimerLocked();
        }

        ClosePoolsSafely(poolsToClose);
    }

    private void OnEvictionTimer()
    {
        List<SpannerPool>? poolsToClose;
        lock (_lock)
        {
            if (_disposed)
            {
                return;
            }
            // The one-shot timer has just fired, so clear armed state even if it fired early.
            _timerArmed = false;
            _timerHeadIdleTicks = 0;
            poolsToClose = CollectPoolsToEvictLocked();
            UpdateTimerLocked();
        }

        ClosePoolsSafely(poolsToClose);
    }

    public void Dispose()
    {
        List<SpannerPool> poolsToClose;
        lock (_lock)
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;

            poolsToClose = new List<SpannerPool>(_entries.Count);
            foreach (var entry in _entries.Values)
            {
                if (entry.Pool != null)
                {
                    entry.Pool.Closed = true;
                    entry.Pool.IdleNode = null;
                    poolsToClose.Add(entry.Pool);
                }
            }

            _entries.Clear();
            _idlePools.Clear();
            _timerArmed = false;
            _timerHeadIdleTicks = 0;
        }
        ClosePoolsSafely(poolsToClose);
        _evictionTimer.Dispose();
    }

    private List<SpannerPool>? CollectPoolsToEvictLocked()
    {
        List<SpannerPool>? poolsToClose = null;
        while (_idlePools.First != null)
        {
            var oldestPool = _idlePools.First.Value;
            var isOverCapacity = _idlePools.Count > _maxIdlePools;
            var isExpired = _timeProvider.GetElapsedTime(oldestPool.IdleSinceTicks) >= _idleTimeout;
            if (!isOverCapacity && !isExpired)
            {
                break;
            }

            _idlePools.RemoveFirst();
            oldestPool.IdleNode = null;
            oldestPool.Closed = true;
            _entries.Remove(oldestPool.ConnectionString);
            poolsToClose ??= [];
            poolsToClose.Add(oldestPool);
        }
        return poolsToClose;
    }

    private void UpdateTimerLocked()
    {
        if (_disposed || _idlePools.First == null)
        {
            DisarmTimerLocked();
            return;
        }

        var headIdleTicks = _idlePools.First.Value.IdleSinceTicks;
        if (_timerArmed && headIdleTicks == _timerHeadIdleTicks)
        {
            return;
        }

        var elapsed = _timeProvider.GetElapsedTime(headIdleTicks);
        var remaining = _idleTimeout - elapsed;
        if (remaining < MinTimerDueTime)
        {
            remaining = MinTimerDueTime;
        }
        else if (remaining > MaxTimerDueTime)
        {
            remaining = MaxTimerDueTime;
        }
        _evictionTimer.Change(remaining, Timeout.InfiniteTimeSpan);
        _timerArmed = true;
        _timerHeadIdleTicks = headIdleTicks;
    }

    private void DisarmTimerLocked()
    {
        if (_timerArmed)
        {
            _evictionTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            _timerArmed = false;
            _timerHeadIdleTicks = 0;
        }
    }

    private static void ClosePoolsSafely(List<SpannerPool>? poolsToClose)
    {
        if (poolsToClose == null)
        {
            return;
        }
        foreach (var pool in poolsToClose)
        {
            pool.CloseLibPoolSafely();
        }
    }
}