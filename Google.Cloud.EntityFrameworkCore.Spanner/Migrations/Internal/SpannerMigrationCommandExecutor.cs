// Copyright 2021 Google LLC
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

using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using IsolationLevel = System.Data.IsolationLevel;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal
{
    internal class SpannerMigrationCommandExecutor : IMigrationCommandExecutor
    {
        public void ExecuteNonQuery(IEnumerable<MigrationCommand> migrationCommands, IRelationalConnection connection)
        {
            ExecuteNonQueryAsync(migrationCommands, connection).WaitWithUnwrappedExceptions();
        }

        public int ExecuteNonQuery(IReadOnlyList<MigrationCommand> migrationCommands, IRelationalConnection connection,
            MigrationExecutionState executionState, bool commitTransaction, IsolationLevel? isolationLevel = null)
        {
            return ExecuteNonQueryAsync(migrationCommands, connection, executionState, commitTransaction, isolationLevel).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public Task ExecuteNonQueryAsync(IEnumerable<MigrationCommand> migrationCommands, IRelationalConnection connection, CancellationToken cancellationToken = default)
        {
            return ExecuteNonQueryAsync(migrationCommands.ToList(), connection, new MigrationExecutionState(),
                commitTransaction: true, IsolationLevel.Unspecified, cancellationToken);
        }

        public async Task<int> ExecuteNonQueryAsync(IReadOnlyList<MigrationCommand> migrationCommands, IRelationalConnection connection,
            MigrationExecutionState executionState, bool commitTransaction, IsolationLevel? isolationLevel = null,
            CancellationToken cancellationToken = default)
        {
            GaxPreconditions.CheckArgument(connection is SpannerRelationalConnection, nameof(connection), "Can only be used with Spanner connections");
            var useIsolationLevel = isolationLevel ?? IsolationLevel.Unspecified;
            var result = 0;
            var statements = migrationCommands.Select(x => x.CommandText).ToArray();
            if (statements.Length == 0)
            {
                return result;
            }
            // Spanner does not support DDL transactions. We therefore ignore the transaction options given
            // for the migration commands. Any DML statements are executed in a separate transaction.
            var ddlStatements = statements.Where(IsDdlStatement).ToArray();
            var otherStatements = statements.Where(x => !IsDdlStatement(x)).ToArray();
            var spannerConnection = (((SpannerRelationalConnection) connection).DbConnection as SpannerRetriableConnection)!;
            if (ddlStatements.Length != 0)
            {
                var cmd = spannerConnection.CreateDdlCommand(ddlStatements[0], ddlStatements.Skip(1).ToArray());
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
            if (otherStatements.Length != 0)
            {
                await using var transaction = await spannerConnection.BeginTransactionAsync(useIsolationLevel, cancellationToken);
                var cmd = spannerConnection.CreateBatchDmlCommand();
                cmd.Transaction = transaction;
                foreach (var statement in otherStatements)
                {
                    cmd.Add(statement);
                }
                var updateCounts = await cmd.ExecuteNonQueryAsync(cancellationToken);
                result = (int) updateCounts.Sum();
                await transaction.CommitAsync(cancellationToken);
            }
            return result;
        }

        private bool IsDdlStatement(string statement)
        {
            return SpannerCommandTextBuilder.FromCommandText(statement).SpannerCommandType == SpannerCommandType.Ddl;
        }
    }
}
