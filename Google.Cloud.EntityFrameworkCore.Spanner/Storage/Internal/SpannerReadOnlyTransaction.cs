using Google.Cloud.Spanner.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    public class SpannerReadOnlyTransaction : SpannerTransactionBase
    {
        private readonly SpannerTransaction _spannerTransaction;

        public SpannerReadOnlyTransaction(SpannerRetriableConnection connection, SpannerTransaction spannerTransaction)
        {
            DbConnection = connection;
            _spannerTransaction = spannerTransaction;
        }

        protected override DbConnection DbConnection { get; }

        public override IsolationLevel IsolationLevel => _spannerTransaction.IsolationLevel;

        public override void Commit() => _spannerTransaction.Commit();

        public override void Rollback() => _spannerTransaction.Rollback();

        protected internal override int ExecuteNonQueryWithRetry(SpannerCommand command) =>
            throw new InvalidOperationException("Non-query operations are not allowed on a read-only transaction");

        protected internal override IEnumerable<long> ExecuteNonQueryWithRetry(SpannerRetriableBatchCommand command) =>
            throw new InvalidOperationException("Non-query operations are not allowed on a read-only transaction");


        protected internal override object ExecuteScalarWithRetry(SpannerCommand command)
        {
            command.Transaction = _spannerTransaction;
            return command.ExecuteScalar();
        }

        protected internal override DbDataReader ExecuteDbDataReaderWithRetry(SpannerCommand command)
        {
            command.Transaction = _spannerTransaction;
            return command.ExecuteReader();
        }
    }
}
