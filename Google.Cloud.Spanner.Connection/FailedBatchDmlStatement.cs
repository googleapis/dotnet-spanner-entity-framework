// Copyright 2021, Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.Spanner.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.Connection
{
    /// <summary>
    /// Represents a Batch DML statement that returned an error during the initial attempt.
    /// Should return the same error and possibly the same update counts during a retry for the
    /// retry to be deemed successful.
    /// </summary>
    internal sealed class FailedBatchDmlStatement : IRetriableStatement
    {
        private readonly SpannerRetriableBatchCommand _command;
        private readonly SpannerException _exception;

        internal FailedBatchDmlStatement(SpannerRetriableBatchCommand command, SpannerException exception)
        {
            _command = command;
            _exception = exception;
        }

        async Task IRetriableStatement.RetryAsync(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds)
        {
            try
            {
                await _command.CreateSpannerBatchCommand().ExecuteNonQueryAsync(cancellationToken);
                // Fallthrough and throw the exception at the end of the method.
            }
            catch (SpannerBatchNonQueryException e)
            {
                // Check that we got the exact same exception and results this time as the previous time.
                if (_exception is SpannerBatchNonQueryException batchException
                    && e.ErrorCode == _exception.ErrorCode
                    && e.Message.Equals(_exception.Message)
                    // A Batch DML statement returns the update counts of the first N statements and the error
                    // that occurred for statement N+1.
                    && e.SuccessfulCommandResults.SequenceEqual(batchException.SuccessfulCommandResults)
                    )
                {
                    return;
                }
            }
            catch (SpannerException e) when (e.ErrorCode != ErrorCode.Aborted)
            {
                // Check that we got the exact same exception during the retry as during the initial attempt.
                // This happens if the Batch DML RPC itself failed, and not one of the DML statements.
                if (!(_exception is SpannerBatchNonQueryException) && SpannerRetriableTransaction.SpannerExceptionsEqualForRetry(e, _exception))
                {
                    return;
                }
            }
            throw new SpannerAbortedDueToConcurrentModificationException();
        }
    }
}
