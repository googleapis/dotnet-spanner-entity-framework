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

using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerMutationBasedModificationCommandBatch : ModificationCommandBatch
    {
        //Note: This class hooks into updates before any DML is generated. It handles the updates by converting
        // each row update into the appropriate SpannerCommand (mutation).  Note the use of the type mapper.

        private readonly IDiagnosticsLogger<DbLoggerCategory.Database.Command> _logger;
        private readonly List<ModificationCommand> _modificationCommands = new List<ModificationCommand>();
        private readonly IRelationalTypeMappingSource _typeMapper;

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public SpannerMutationBasedModificationCommandBatch(IRelationalTypeMappingSource typeMapper,
            IDiagnosticsLogger<DbLoggerCategory.Database.Command> logger)
        {
            _typeMapper = typeMapper;
            _logger = logger;
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override IReadOnlyList<ModificationCommand> ModificationCommands => _modificationCommands;

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override bool AddCommand(ModificationCommand modificationCommand)
        {
            _modificationCommands.Add(modificationCommand);
            return true;
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override void Execute(IRelationalConnection connection)
        {
            //TODO(benwu): call sync down the stack.
            Task.Run(() => ExecuteAsync(connection)).WaitWithUnwrappedExceptions();
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override async Task ExecuteAsync(IRelationalConnection connection,
            CancellationToken cancellationToken = new CancellationToken())
        {
            GaxPreconditions.CheckArgument(connection.DbConnection is SpannerRetriableConnection, nameof(connection), "");
            var spannerConnection = (SpannerRetriableConnection)connection.DbConnection;
            if (!(connection.CurrentTransaction?.GetDbTransaction() is SpannerRetriableTransaction transaction))
            {
                throw new InvalidOperationException("");
            }
            var tasks = _modificationCommands.Select(
                x => CreateSpannerCommand(spannerConnection, transaction, x)
                    .ExecuteNonQueryAsync(cancellationToken)).ToArray();

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        private SpannerCommand CreateSpannerCommand(
            SpannerRetriableConnection spannerConnection,
            SpannerRetriableTransaction transaction,
            ModificationCommand modificationCommand)
        {
            SpannerCommand cmd = modificationCommand.EntityState switch
            {
                EntityState.Deleted => spannerConnection.SpannerConnection.CreateDeleteCommand(modificationCommand.TableName),
                EntityState.Modified => spannerConnection.SpannerConnection.CreateUpdateCommand(modificationCommand.TableName),
                EntityState.Added => spannerConnection.SpannerConnection.CreateInsertCommand(modificationCommand.TableName),
                _ => throw new NotSupportedException($"Modification type {modificationCommand.EntityState} is not supported."),
            };
            // TODO: Integrate with Spanner logging
            cmd.Transaction = transaction.SpannerTransaction;
            foreach (var columnModification in modificationCommand.ColumnModifications)
            {
                if (string.IsNullOrEmpty(columnModification.Property.GetComputedColumnSql()))
                {
                    cmd.Parameters.Add(
                        _typeMapper.GetMapping(columnModification.Property).CreateParameter(cmd,
                            columnModification.ColumnName,
                            columnModification.UseOriginalValueParameter
                                ? columnModification.OriginalValue
                                : columnModification.Value, columnModification.Property.IsNullable));
                }
            }
            return cmd;
        }
    }
}
