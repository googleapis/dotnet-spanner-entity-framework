using Google.Cloud.Spanner.Data;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// Represents a DML statement that successfully returned an update count.
    /// </summary>
    internal sealed class RetriableDmlStatement : IRetriableStatement
    {
        private readonly SpannerCommand _command;
        private readonly long _updateCount;

        internal RetriableDmlStatement(SpannerCommand command, long updateCount)
        {
            _command = (SpannerCommand)command.Clone();
            _updateCount = updateCount;
        }

        async Task IRetriableStatement.Retry(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds)
        {
            try
            {
                _command.Transaction = transaction.SpannerTransaction;
                // The DML statement should return the same update count as during the initial attempt
                // for the retry to be deemed successful.
                if (await _command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) != _updateCount)
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
