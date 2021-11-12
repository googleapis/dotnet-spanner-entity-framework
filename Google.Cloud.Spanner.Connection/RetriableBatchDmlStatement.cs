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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.Spanner.Connection
{
    /// <summary>
    /// Represents a Batch DML statement that returned a list of update counts.
    /// Should return the same update counts during a retry for it to be deemed a successful retry.
    /// </summary>
    internal sealed class RetriableBatchDmlStatement : IRetriableStatement
    {
        private readonly SpannerRetriableBatchCommand _command;
        private readonly IReadOnlyList<long> _updateCounts;

        internal RetriableBatchDmlStatement(SpannerRetriableBatchCommand command, IReadOnlyList<long> updateCounts)
        {
            // TODO: Do we need to make this a clone?
            _command = command;
            _updateCounts = updateCounts;
        }

        async Task IRetriableStatement.RetryAsync(SpannerRetriableTransaction transaction, CancellationToken cancellationToken, int timeoutSeconds)
        {
            try
            {
                _command.Transaction = transaction;
                if (!_updateCounts.SequenceEqual(await _command.CreateSpannerBatchCommand().ExecuteNonQueryAsync(cancellationToken)))
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
