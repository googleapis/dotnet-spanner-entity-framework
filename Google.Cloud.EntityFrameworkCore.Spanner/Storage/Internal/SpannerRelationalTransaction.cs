using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Data.Common;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;

public class SpannerRelationalTransaction : RelationalTransaction
{
    public SpannerRelationalTransaction([NotNull] IRelationalConnection connection, [NotNull] DbTransaction transaction, Guid transactionId, IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger, bool transactionOwned, [NotNull] ISqlGenerationHelper sqlGenerationHelper) :
        base(connection, transaction, transactionId, logger, transactionOwned, sqlGenerationHelper)
    {
    }
    
    public override bool SupportsSavepoints => false;
}