using Google.Cloud.Spanner.Data;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage
{
    public abstract class SpannerTransactionBase : DbTransaction
    {
        protected internal abstract int ExecuteNonQueryWithRetry(SpannerCommand command);

        protected internal abstract IEnumerable<long> ExecuteNonQueryWithRetry(SpannerRetriableBatchCommand command);

        protected internal abstract object ExecuteScalarWithRetry(SpannerCommand command);

        protected internal abstract DbDataReader ExecuteDbDataReaderWithRetry(SpannerCommand command);
    }
}
