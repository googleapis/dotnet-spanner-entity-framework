// Copyright 2020, Google Inc. All rights reserved.
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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Update.Internal;
using OpenTelemetry.Trace;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
#pragma warning disable EF1001
    /// <inheritdoc />
    internal class SpannerBatchExecutor : BatchExecutor
    {
        /// <summary>
        /// Only for internal use.
        /// </summary>
        public SpannerBatchExecutor([NotNull] ICurrentDbContext currentContext, IDiagnosticsLogger<DbLoggerCategory.Update> updateLogger) :
            base(currentContext, updateLogger)
        {
        }

        /// <inheritdoc />
        public override int Execute(IEnumerable<ModificationCommandBatch> commandBatches, IRelationalConnection connection)
        {
            // Convert the list of batches to a list to prevent it from being re-generated each time that we iterate over the enumerator.
            var batchesList = commandBatches.ToList();
            var tracer = TracerProviderExtension.GetTracer();
            using var span = tracer.StartActiveSpan(TracerProviderExtension.SPAN_NAME_SAVECHANGES);
            try
            {
#pragma warning disable EF1001
                base.Execute(batchesList, connection);
#pragma warning restore EF1001
                foreach (var batch in batchesList)
                {
                    if (batch is SpannerModificationCommandBatch spannerModificationCommandBatch)
                    {
                        spannerModificationCommandBatch.PropagateResults(connection);
                    }
                }
                span.SetStatus(Status.Ok);
            }
            catch (Exception ex)
            {
                span.SetStatus(Status.Error.WithDescription(ex.Message));
                throw;
            }
            finally
            {
                span.End();
            }
            return SpannerCount(batchesList);
        }

        /// <inheritdoc />
        public override async Task<int> ExecuteAsync(IEnumerable<ModificationCommandBatch> commandBatches, IRelationalConnection connection, CancellationToken cancellationToken = default)
        {
            // Convert the list of batches to a list to prevent it from being re-generated each time that we iterate over the enumerator.
            var batchesList = commandBatches.ToList();
            var tracer = TracerProviderExtension.GetTracer();
            using var span = tracer.StartActiveSpan(TracerProviderExtension.SPAN_NAME_SAVECHANGES);
            try
            {
#pragma warning disable EF1001
                await base.ExecuteAsync(batchesList, connection, cancellationToken);
#pragma warning restore EF1001
                // Results that need to be propagated after an update are executed after the batch has been saved.
                // This ensures that when implicit transactions are being used the updated value is fetched after the
                // transaction has been committed. This makes it possible to use mutations for implicit transactions
                // and still automatically propagate computed columns.
                foreach (var batch in batchesList)
                {
                    if (batch is SpannerModificationCommandBatch spannerModificationCommandBatch)
                    {
                        await spannerModificationCommandBatch.PropagateResultsAsync(connection, cancellationToken);
                    }
                }
                span.SetStatus(Status.Ok);
            }
            catch (Exception ex)
            {
                span.SetStatus(Status.Error.WithDescription(ex.Message));
                throw;
            }
            finally
            {
                span.End();
            }
            return SpannerCount(batchesList);
        }

        private int SpannerCount(List<ModificationCommandBatch> commandBatches)
        {
            var spannerCount = 0;
            foreach (var commandBatch in commandBatches)
            {
                if (commandBatch is SpannerModificationCommandBatch spannerCommandBatch)
                {
                    spannerCount += (int)spannerCommandBatch.UpdateCounts.Sum();
                }
            }
            return spannerCount;
        }
    }
}
