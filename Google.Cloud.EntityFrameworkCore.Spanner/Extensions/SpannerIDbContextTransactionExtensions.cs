using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions
{
    public static class SpannerIDbContextTransactionExtensions
    {
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
