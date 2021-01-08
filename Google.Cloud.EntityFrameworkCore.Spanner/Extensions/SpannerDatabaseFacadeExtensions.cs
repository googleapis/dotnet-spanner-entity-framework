using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Extensions
{
    public static class SpannerDatabaseFacadeExtensions
    {
        public static IDbContextTransaction BeginReadOnlyTransaction([NotNull] this DatabaseFacade databaseFacade) =>
            BeginReadOnlyTransaction(databaseFacade, TimestampBound.Strong);

        public static IDbContextTransaction BeginReadOnlyTransaction([NotNull] this DatabaseFacade databaseFacade, [NotNull] TimestampBound timestampBound)
        {
            var transactionManager = databaseFacade.GetService<IDbContextTransactionManager>();
            if (transactionManager is SpannerRelationalConnection spannerRelationalConnection)
            {
                return spannerRelationalConnection.BeginReadOnlyTransaction(timestampBound);
            }
            throw new InvalidOperationException("Read-only transactions can only be started for Spanner databases");
        }

        public static Task<IDbContextTransaction> BeginReadOnlyTransactionAsync([NotNull] this DatabaseFacade databaseFacade, CancellationToken cancellationToken = default) =>
            BeginReadOnlyTransactionAsync(databaseFacade, TimestampBound.Strong);

        public static Task<IDbContextTransaction> BeginReadOnlyTransactionAsync([NotNull] this DatabaseFacade databaseFacade, [NotNull] TimestampBound timestampBound)
        {
            var transactionManager = databaseFacade.GetService<IDbContextTransactionManager>();
            if (transactionManager is SpannerRelationalConnection spannerRelationalConnection)
            {
                return spannerRelationalConnection.BeginReadOnlyTransactionAsync(timestampBound);
            }
            throw new InvalidOperationException("Read-only transactions can only be started for Spanner databases");
        }
    }
}
