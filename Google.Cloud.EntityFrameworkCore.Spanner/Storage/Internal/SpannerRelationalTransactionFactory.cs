using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Data.Common;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;

public class SpannerRelationalTransactionFactory : RelationalTransactionFactory
{
    public SpannerRelationalTransactionFactory(RelationalTransactionFactoryDependencies dependencies) :
        base(dependencies)
    {
    }

    /// <inheritdoc/>
    public override RelationalTransaction Create(
        IRelationalConnection connection,
        DbTransaction transaction,
        Guid transactionId,
        IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger,
        bool transactionOwned)
        => new SpannerRelationalTransaction(connection, transaction, transactionId, logger, transactionOwned, Dependencies.SqlGenerationHelper);
    
}