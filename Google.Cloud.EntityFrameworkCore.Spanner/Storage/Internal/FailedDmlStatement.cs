using Google.Cloud.Spanner.Data;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// Represents a DML statement that failed with an error during the initial attempt.
    /// It should return the same error during a retry to be deemed a successful retry.
    /// </summary>
    internal sealed class FailedDmlStatement : IRetriableStatement
    {
        private readonly SpannerCommand _command;
        private readonly SpannerException _exception;

        internal FailedDmlStatement(SpannerCommand command, SpannerException exception)
        {
            _command = (SpannerCommand)command.Clone();
            _exception = exception;
        }

        async Task IRetriableStatement.Retry(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds)
        {
            try
            {
                _command.Transaction = transaction.SpannerTransaction;
                await _command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                // Fallthrough and throw the exception at the end of the method.
            }
            catch (SpannerException e) when (e.ErrorCode != ErrorCode.Aborted)
            {
                // Check that we got the exact same exception this time as the previous time.
                if (SpannerRetriableTransaction.SpannerExceptionsEqualForRetry(e, _exception))
                {
                    return;
                }
            }
            throw new SpannerAbortedDueToConcurrentModificationException();
        }
    }
}
