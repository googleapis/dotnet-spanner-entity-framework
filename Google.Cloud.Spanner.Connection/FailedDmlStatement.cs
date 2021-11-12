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
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.Connection
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

        async Task IRetriableStatement.RetryAsync(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds)
        {
            try
            {
                _command.Transaction = transaction.SpannerTransaction;
                await _command.ExecuteNonQueryAsync(cancellationToken);
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
