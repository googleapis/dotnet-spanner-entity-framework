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
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Storage;
using System;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions
{
    /// <summary>
    /// Extensions for Cloud Spanner transactions.
    /// </summary>
    public static class SpannerIDbContextTransactionExtensions
    {
        /// <summary>
        /// Disables internal retries for Aborted errors for the transaction. Internal retries are enabled by default.
        /// This method may only be called for read/write Spanner transactions.
        /// </summary>
        /// <param name="dbContextTransaction">The transaction to disable internal retries for.</param>
        /// <exception cref="ArgumentException">If the transaction is not a read/write Spanner transaction</exception>
        public static void DisableInternalRetries([NotNull] this IDbContextTransaction dbContextTransaction)
        {
            GaxPreconditions.CheckArgument(dbContextTransaction.GetDbTransaction() is SpannerRetriableTransaction, nameof(dbContextTransaction), "Must be a read/write Spanner transaction");
            (dbContextTransaction.GetDbTransaction() as SpannerRetriableTransaction).EnableInternalRetries = false;
        }

        /// <summary>
        /// The commit timestamp of the transaction. This property is only valid for read/write Spanner transactions that have committed.
        /// </summary>
        /// <param name="dbContextTransaction"></param>
        /// <returns>The commit timestamp of the transaction</returns>
        /// <exception cref="ArgumentException">If the transaction is not a read/write Spanner transaction</exception>
        /// <exception cref="InvalidOperationException">If the Spanner transaction has not committed</exception>
        public static DateTime GetCommitTimestamp([NotNull] this IDbContextTransaction dbContextTransaction)
        {
            GaxPreconditions.CheckArgument(dbContextTransaction.GetDbTransaction() is SpannerRetriableTransaction, nameof(dbContextTransaction), "Must be a read/write Spanner transaction");
            return (dbContextTransaction.GetDbTransaction() as SpannerRetriableTransaction).CommitTimestamp;
        }

        // TODO: Add method for GetReadTimestamp for read-only transactions.
        // That requires an addition to the client library to actually be able
        // to get the read timestamp of a read-only transaction.
    }
}
