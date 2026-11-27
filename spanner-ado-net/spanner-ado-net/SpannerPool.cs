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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.Spanner.V1;
using Google.Cloud.SpannerLib;
using Google.Cloud.SpannerLib.Grpc;
using Google.Rpc;

namespace Google.Cloud.Spanner.DataProvider;

internal class SpannerPool
{
    private static bool UseNativeLib => bool.Parse(Environment.GetEnvironmentVariable("SPANNER_ADO_USE_NATIVE_LIB") ?? "false");
    
    private static int NumChannels => int.Parse(Environment.GetEnvironmentVariable("SPANNER_ADO_NUM_CHANNELS") ?? "4");
    
    private static GrpcLibSpanner.CommunicationStyle CommunicationStyle => Enum.Parse<GrpcLibSpanner.CommunicationStyle>(Environment.GetEnvironmentVariable("SPANNER_ADO_COMMUNICATION_STYLE") ?? nameof(GrpcLibSpanner.CommunicationStyle.BidiStreaming));
    
    private static ISpannerLib? _gRpcSpannerLib;

    private static ISpannerLib GrpcSpannerLib
    {
        get
        {
            _gRpcSpannerLib ??= new GrpcLibSpanner(numChannels: NumChannels, communicationStyle: CommunicationStyle);
            return _gRpcSpannerLib;
        }
    }
        
    private static readonly ConcurrentDictionary<string, SpannerPool> Pools = new();

    private static readonly string[] DialectQueries = ["SHOW VARIABLE DATABASE_DIALECT", "SHOW DATABASE_DIALECT"];

    [MethodImpl(MethodImplOptions.Synchronized)]
    internal static SpannerPool GetOrCreate(string dsn)
    {
        if (Pools.TryGetValue(dsn, out var value))
        {
            return value;
        }
        var pool = Pool.Create(GrpcSpannerLib, dsn);
        var spannerPool = new SpannerPool(dsn, pool);
        Pools[dsn] = spannerPool;
        return spannerPool;
    }

    [MethodImpl(MethodImplOptions.Synchronized)]
    internal static void CloseSpannerLib()
    {
        foreach (var pool in Pools.Values)
        {
            pool.Close();
        }
        Pools.Clear();
        GrpcSpannerLib.Dispose();
        _gRpcSpannerLib = null;
    }

    private readonly string _dsn;
    
    private readonly Pool _libPool;

    private readonly SemaphoreSlim _dialectSemaphore = new(1, 1);

    // TODO: Return the database dialect from SpannerLib when creating a Pool so client-side probing is unnecessary.
    private volatile SpannerDialect? _dialect;

    internal SpannerPool(string dsn, Pool libPool)
    {
        _dsn = dsn;
        _libPool = libPool;
    }

    internal void Close()
    {
        _libPool.Close();
        Pools.Remove(_dsn, out _);
    }

    internal Connection CreateConnection()
    {
        return _libPool.CreateConnection();
    }

    internal SpannerDialect GetDialect()
    {
        if (_dialect is { } cachedDialect)
        {
            return cachedDialect;
        }

        _dialectSemaphore.Wait();
        try
        {
            if (_dialect is { } dialect)
            {
                return dialect;
            }

            var detectedDialect = SpannerDbException.TranslateException(DetectDialect);
            _dialect = detectedDialect;
            return detectedDialect;
        }
        finally
        {
            _dialectSemaphore.Release();
        }
    }

    internal async Task<SpannerDialect> GetDialectAsync(CancellationToken cancellationToken = default)
    {
        if (_dialect is { } cachedDialect)
        {
            return cachedDialect;
        }

        await _dialectSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_dialect is { } dialect)
            {
                return dialect;
            }

            var detectedDialect = await SpannerDbException.TranslateException(DetectDialectAsync(cancellationToken)).ConfigureAwait(false);
            _dialect = detectedDialect;
            return detectedDialect;
        }
        finally
        {
            _dialectSemaphore.Release();
        }
    }

    private SpannerDialect DetectDialect()
    {
        // Both SHOW statements are handled client-side in SpannerLib using the dialect metadata
        // already cached on the underlying pool/connection; they do NOT execute an RPC on Spanner.
        using var connection = _libPool.CreateConnection();
        foreach (var sql in DialectQueries)
        {
            if (TryQueryDialect(connection, sql) is { } dialect)
            {
                return dialect;
            }
        }
        throw new InvalidOperationException("Failed to determine the database dialect.");
    }

    private async Task<SpannerDialect> DetectDialectAsync(CancellationToken cancellationToken)
    {
        // Both SHOW statements are handled client-side in SpannerLib using the dialect metadata
        // already cached on the underlying pool/connection; they do NOT execute an RPC on Spanner.
        var connection = _libPool.CreateConnection();
        await using (connection.ConfigureAwait(false))
        {
            foreach (var sql in DialectQueries)
            {
                if (await TryQueryDialectAsync(connection, sql, cancellationToken).ConfigureAwait(false) is { } dialect)
                {
                    return dialect;
                }
            }
            throw new InvalidOperationException("Failed to determine the database dialect.");
        }
    }

    private static SpannerDialect? TryQueryDialect(Connection connection, string sql)
    {
        try
        {
            using var rows = connection.Execute(new ExecuteSqlRequest { Sql = sql });
            var row = rows.Next();
            var value = row?.Values.Count > 0 ? row.Values[0].StringValue : null;
            return ParseDialect(value);
        }
        catch (SpannerException exception) when (exception.Code == Code.InvalidArgument)
        {
            return null;
        }
    }

    private static async Task<SpannerDialect?> TryQueryDialectAsync(Connection connection, string sql, CancellationToken cancellationToken)
    {
        try
        {
            var rows = await connection.ExecuteAsync(new ExecuteSqlRequest { Sql = sql }, prefetchRows: 1, cancellationToken).ConfigureAwait(false);
            await using (rows.ConfigureAwait(false))
            {
                var row = await rows.NextAsync(cancellationToken).ConfigureAwait(false);
                var value = row?.Values.Count > 0 ? row.Values[0].StringValue : null;
                return ParseDialect(value);
            }
        }
        catch (SpannerException exception) when (exception.Code == Code.InvalidArgument)
        {
            return null;
        }
    }

    private static SpannerDialect? ParseDialect(string? value) => value switch
    {
        "GOOGLE_STANDARD_SQL" => SpannerDialect.GoogleStandardSql,
        "POSTGRESQL" => SpannerDialect.Postgresql,
        _ => null,
    };
}