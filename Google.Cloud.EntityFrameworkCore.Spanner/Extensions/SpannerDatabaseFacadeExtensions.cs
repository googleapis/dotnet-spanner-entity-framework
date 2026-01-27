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

using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions
{
    /// <summary>
    /// Extensions to <see cref="DatabaseFacade"/> for Cloud Spanner databases.
    /// </summary>
    public static class SpannerDatabaseFacadeExtensions
    {
        /// <summary>
        /// Returns the underlying SpannerConnection of this database.
        /// </summary>
        /// <param name="databaseFacade">The Cloud Spanner database to get the connection from</param>
        /// <returns>The underlying SpannerConnection of the database</returns>
        /// <throws>ArgumentException if this DatabaseFacade is not connected to Cloud Spanner</throws>
        public static SpannerConnection GetSpannerConnection([NotNull] this DatabaseFacade databaseFacade)
        {
            if (!(databaseFacade.GetDbConnection() is SpannerRetriableConnection connection))
            {
                throw new ArgumentException("The database is not Cloud Spanner");
            }
            return connection.SpannerConnection;
        }

        /// <summary>
        /// Begins a read/write transaction with the given transaction tag on a Cloud Spanner database.
        /// </summary>
        /// <param name="databaseFacade">The Spanner database to begin the transaction on</param>
        /// <param name="tag">The transaction tag to use for the transaction</param>
        /// <returns>A read/write transaction using the given transaction tag</returns>
        public static IDbContextTransaction BeginTransaction([NotNull] this DatabaseFacade databaseFacade, string tag)
        {
            var transactionManager = databaseFacade.GetService<IDbContextTransactionManager>();
            if (transactionManager is SpannerRelationalConnection spannerRelationalConnection)
            {
                return spannerRelationalConnection.BeginTransaction(tag);
            }
            throw new InvalidOperationException("Transaction tags can only be used with Spanner databases");
        }

        /// <summary>
        /// Begins a read/write transaction with the given transaction tag on a Cloud Spanner database.
        /// </summary>
        /// <param name="databaseFacade">The Spanner database to begin the transaction on</param>
        /// <param name="tag">The transaction tag to use for the transaction</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> cancellation token to monitor for the asynchronous operation.</param>
        /// <returns>A read/write transaction using the given transaction tag</returns>
        public static Task<IDbContextTransaction> BeginTransactionAsync([NotNull] this DatabaseFacade databaseFacade, string tag, CancellationToken cancellationToken = default)
        {
            var transactionManager = databaseFacade.GetService<IDbContextTransactionManager>();
            if (transactionManager is SpannerRelationalConnection spannerRelationalConnection)
            {
                return spannerRelationalConnection.BeginTransactionAsync(tag, cancellationToken);
            }
            throw new InvalidOperationException("Transaction tags can only be used with Spanner databases");
        }
        
        /// <summary>
        /// Begins a read-only transaction for a Cloud Spanner database.
        /// </summary>
        /// <param name="databaseFacade">The Cloud Spanner database to begin the transaction on</param>
        /// <returns>A read-only transaction using <see cref="TimestampBoundMode.Strong"/></returns>
        public static IDbContextTransaction BeginReadOnlyTransaction([NotNull] this DatabaseFacade databaseFacade) =>
            BeginReadOnlyTransaction(databaseFacade, TimestampBound.Strong);

        /// <summary>
        /// Begins a read-only transaction for a Cloud Spanner database using the specified <see cref="TimestampBound"/>
        /// </summary>
        /// <param name="databaseFacade">The Cloud Spanner database to begin the transaction on</param>
        /// <param name="timestampBound">The timestamp to use for the read-only transaction</param>
        /// <returns>A read-only transaction using the specified <see cref="TimestampBound"/></returns>
        /// <exception cref="InvalidOperationException">If the database is not a Cloud Spanner database.</exception>
        public static IDbContextTransaction BeginReadOnlyTransaction([NotNull] this DatabaseFacade databaseFacade, [NotNull] TimestampBound timestampBound)
            => BeginReadOnlyTransactionAsync(databaseFacade, timestampBound).ResultWithUnwrappedExceptions();

        /// <summary>
        /// Begins a read-only transaction for a Cloud Spanner database.
        /// </summary>
        /// <param name="databaseFacade">The Cloud Spanner database to begin the transaction on</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> cancellation token to monitor for the asynchronous operation.</param>
        /// <returns>A read-only transaction using <see cref="TimestampBoundMode.Strong"/></returns>
        /// <exception cref="InvalidOperationException">If the database is not a Cloud Spanner database.</exception>
        public static Task<IDbContextTransaction> BeginReadOnlyTransactionAsync([NotNull] this DatabaseFacade databaseFacade, CancellationToken cancellationToken = default) =>
            BeginReadOnlyTransactionAsync(databaseFacade, TimestampBound.Strong, cancellationToken);

        /// <summary>
        /// Begins a read-only transaction for a Cloud Spanner database using the specified <see cref="TimestampBound"/>
        /// </summary>
        /// <param name="databaseFacade">The Cloud Spanner database to begin the transaction on</param>
        /// <param name="timestampBound">The timestamp to use for the read-only transaction</param>
        /// <param name="cancellationToken">A <see cref="CancellationToken" /> cancellation token to monitor for the asynchronous operation.</param>
        /// <returns>A read-only transaction using the specified <see cref="TimestampBound"/></returns>
        /// <exception cref="InvalidOperationException">If the database is not a Cloud Spanner database.</exception>
        public static Task<IDbContextTransaction> BeginReadOnlyTransactionAsync([NotNull] this DatabaseFacade databaseFacade, [NotNull] TimestampBound timestampBound, CancellationToken cancellationToken = default)
        {
            var transactionManager = databaseFacade.GetService<IDbContextTransactionManager>();
            if (transactionManager is SpannerRelationalConnection spannerRelationalConnection)
            {
                return spannerRelationalConnection.BeginReadOnlyTransactionAsync(timestampBound, cancellationToken);
            }
            throw new InvalidOperationException("Read-only transactions can only be started for Spanner databases");
        }
    }
}
