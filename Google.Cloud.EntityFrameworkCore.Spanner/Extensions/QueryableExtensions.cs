// Copyright 2021, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions
{
    public static class QueryableExtensions
    {
        /// <summary>
        /// Instructs Entity Framework Core to execute the query with the given timestamp bound.
        /// This is only supported outside of transactions.
        /// 
        /// Calling this method for a queryable during a transaction is a no-op, as the query
        /// will always use the staleness of the current transaction. 
        /// </summary>
        public static IQueryable<TEntity> WithTimestampBound<TEntity>([NotNull] this IQueryable<TEntity> queryable,
            TimestampBound timestampBound)
        {
            return timestampBound.Mode switch
            {
                TimestampBoundMode.Strong =>
                    // No action needed, this is the default.
                    queryable,
                TimestampBoundMode.ReadTimestamp => WithReadTimestamp(queryable, timestampBound.Timestamp),
                TimestampBoundMode.MinReadTimestamp => WithMinReadTimestamp(queryable, timestampBound.Timestamp),
                TimestampBoundMode.ExactStaleness => WithExactStaleness(queryable, timestampBound.Staleness),
                TimestampBoundMode.MaxStaleness => WithMaxStaleness(queryable, timestampBound.Staleness),
                _ => queryable
            };
        }
        
        private static IQueryable<TEntity> WithMaxStaleness<TEntity>([NotNull] this IQueryable<TEntity> queryable, TimeSpan maxStaleness)
        {
            return queryable.TagWith($"max_staleness: {maxStaleness.TotalSeconds}");
        }
        
        private static IQueryable<TEntity> WithExactStaleness<TEntity>([NotNull] this IQueryable<TEntity> queryable, TimeSpan exactStaleness)
        {
            return queryable.TagWith($"exact_staleness: {exactStaleness.TotalSeconds}");
        }
        
        private static IQueryable<TEntity> WithMinReadTimestamp<TEntity>([NotNull] this IQueryable<TEntity> queryable, DateTime minReadTimestamp)
        {
            return queryable.TagWith($"min_read_timestamp: {minReadTimestamp.ToUniversalTime():yyyy-MM-ddTHH:mm:ss.fffffffZ}");
        }
        
        private static IQueryable<TEntity> WithReadTimestamp<TEntity>([NotNull] this IQueryable<TEntity> queryable, DateTime readTimestamp)
        {
            return queryable.TagWith($"read_timestamp: {readTimestamp.ToUniversalTime():yyyy-MM-ddTHH:mm:ss.fffffffZ}");
        }
    }
    
    internal class TimestampBoundHintCommandInterceptor : DbCommandInterceptor
    {
        internal static readonly TimestampBoundHintCommandInterceptor TimestampBoundHintInterceptor = new TimestampBoundHintCommandInterceptor();
        
        private static readonly List<TimestampBoundHint> s_supportedHints = new List<TimestampBoundHint>
        {
            new ExactStalenessHint(),
            new MaxStalenessHint(),
            new ReadTimestampHint(),
            new MinReadTimestampHint()
        };

        private TimestampBoundHintCommandInterceptor()
        {
        }

        public override InterceptionResult<DbDataReader> ReaderExecuting(
            DbCommand command,
            CommandEventData eventData,
            InterceptionResult<DbDataReader> result)
        {
            if (command is SpannerRetriableCommand cmd)
            {
                ManipulateCommand(cmd);
            }
            return result;
        }

        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
            DbCommand command,
            CommandEventData eventData,
            InterceptionResult<DbDataReader> result,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult(ReaderExecuting(command, eventData, result));
        }

        private static void ManipulateCommand(SpannerRetriableCommand command)
        {
            var hint = s_supportedHints.FirstOrDefault(hint => hint.IsHint(command.CommandText));
            if (hint != null)
            {
                try
                {
                    command.TimestampBound = hint.CreateTimestampBound(command.CommandText);
                }
                catch (Exception)
                {
                    // Ignore any invalid timestamp bound in the comment.
                    // That could happen if someone by chance happened to manually add a comment that is the same
                    // as a timestamp bound hint, but with an invalid value.
                }
            }
        }
    }

    internal abstract class TimestampBoundHint
    {
        protected abstract string Hint { get; }

        internal bool IsHint(string query)
        {
            return query.StartsWith(Hint, StringComparison.Ordinal);
        }

        internal TimestampBound CreateTimestampBound(string query)
        {
            var index = query.IndexOf(Environment.NewLine, StringComparison.Ordinal);
            var length = index - Hint.Length;
            if (length > 0)
            {
                var value = query.Substring(Hint.Length, length).Trim();
                return ParseStaleness(value);
            }
            return null;
        }

        protected abstract TimestampBound ParseStaleness(string value);
    }

    internal class MaxStalenessHint : TimestampBoundHint
    {
        protected override string Hint => "-- max_staleness:";
        
        protected override TimestampBound ParseStaleness(string value)
        {
            return double.TryParse(value, out var seconds) ? TimestampBound.OfMaxStaleness(TimeSpan.FromSeconds(seconds)) : null;
        }
    }

    internal class ExactStalenessHint : TimestampBoundHint
    {
        protected override string Hint => "-- exact_staleness:";
        
        protected override TimestampBound ParseStaleness(string value)
        {
            return double.TryParse(value, out var seconds) ? TimestampBound.OfExactStaleness(TimeSpan.FromSeconds(seconds)) : null;
        }
    }

    internal class ReadTimestampHint : TimestampBoundHint
    {
        protected override string Hint => "-- read_timestamp:";
        
        protected override TimestampBound ParseStaleness(string value)
        {
            return DateTime.TryParse(value, out var timestamp) ? TimestampBound.OfReadTimestamp(timestamp.ToUniversalTime()) : null;
        }
    }

    internal class MinReadTimestampHint : TimestampBoundHint
    {
        protected override string Hint => "-- min_read_timestamp:";
        
        protected override TimestampBound ParseStaleness(string value)
        {
            return DateTime.TryParse(value, out var timestamp) ? TimestampBound.OfMinReadTimestamp(timestamp.ToUniversalTime()) : null;
        }
    }
}