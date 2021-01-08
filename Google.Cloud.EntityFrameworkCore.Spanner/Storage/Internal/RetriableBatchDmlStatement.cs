using Google.Cloud.Spanner.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// Represents a Batch DML statement that returned a list of update counts.
    /// Should return the same update counts during a retry for it to be deemed a successful retry.
    /// </summary>
    internal sealed class RetriableBatchDmlStatement : IRetriableStatement
    {
        private readonly SpannerRetriableBatchCommand _command;
        private readonly IEnumerable<long> _updateCounts;

        internal RetriableBatchDmlStatement(SpannerRetriableBatchCommand command, IEnumerable<long> updateCounts)
        {
            // TODO: Do we need to make this a clone?
            _command = command;
            _updateCounts = updateCounts;
        }

        async Task IRetriableStatement.Retry(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds)
        {
            try
            {
                if (!_updateCounts.SequenceEqual(await _command.CreateSpannerBatchCommand().ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false)))
                {
                    throw new SpannerAbortedDueToConcurrentModificationException();
                }
            }
            catch (SpannerException e) when (e.ErrorCode != ErrorCode.Aborted)
            {
                throw new SpannerAbortedDueToConcurrentModificationException();
            }
        }
    }
}
