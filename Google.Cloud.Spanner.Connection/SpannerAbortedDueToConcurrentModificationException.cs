﻿// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Cloud.Spanner.Data;

namespace Google.Cloud.Spanner.Connection
{
    /// <summary>
    /// Represents an error that will be thrown if a SpannerRetriableTransaction executes a retry and
    /// determines that the data that the transaction is operating on has been modified by a different
    /// transaction and must therefore abort the retry attempt.
    /// </summary>
    public class SpannerAbortedDueToConcurrentModificationException : SpannerException
    {
        internal SpannerAbortedDueToConcurrentModificationException()
            : base(ErrorCode.Aborted, "Transaction aborted due to a concurrent modification")
        {
        }

        internal SpannerAbortedDueToConcurrentModificationException(string message)
            : base(ErrorCode.Aborted, $"Transaction aborted due to a concurrent modification: {message}")
        {
        }
    }
}
