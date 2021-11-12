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

        async Task IRetriableStatement.RetryAsync(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds)
        {
            try
            {
                _command.Transaction = transaction.SpannerTransaction;
                // The DML statement should return the same update count as during the initial attempt
                // for the retry to be deemed successful.
                if (await _command.ExecuteNonQueryAsync(cancellationToken) != _updateCount)
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
