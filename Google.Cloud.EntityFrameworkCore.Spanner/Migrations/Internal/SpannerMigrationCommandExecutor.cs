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
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal
{
    internal class SpannerMigrationCommandExecutor(IExecutionStrategy executionStrategy) : IMigrationCommandExecutor
    {
        public void ExecuteNonQuery(IEnumerable<MigrationCommand> migrationCommands, IRelationalConnection connection)
        {
            ExecuteNonQuery(migrationCommands.ToList(), connection, new MigrationExecutionState(), commitTransaction: true);
        }

        public int ExecuteNonQuery(IReadOnlyList<MigrationCommand> migrationCommands, IRelationalConnection connection, MigrationExecutionState executionState, bool commitTransaction, System.Data.IsolationLevel? isolationLevel = null)
        {
            GaxPreconditions.CheckArgument(connection is SpannerRelationalConnection, nameof(connection), "Can only be used with Spanner connections");
            
            var inUserTransaction = connection.CurrentTransaction is not null && executionState.Transaction == null;
            if (inUserTransaction
                && (migrationCommands.Any(x => x.TransactionSuppressed) || executionStrategy.RetriesOnFailure))
            {
                throw new NotSupportedException("Cannot execute transaction suppressed migration commands in user transaction.");
            }

            var cancellationToken = CancellationToken.None;
            using var transactionScope = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);

            return executionStrategy.ExecuteAsync(
                (migrationCommands, connection, inUserTransaction, commitTransaction, isolationLevel),
                static (_, s, ct) => ExecuteInternalAsync(
                    s.migrationCommands,
                    s.connection,
                    beginTransaction: !s.inUserTransaction,
                    commitTransaction: !s.inUserTransaction && s.commitTransaction,
                    s.isolationLevel,
                    ct),
                verifySucceeded: null, cancellationToken).ResultWithUnwrappedExceptions();
        }

        public async Task ExecuteNonQueryAsync(IEnumerable<MigrationCommand> migrationCommands, IRelationalConnection connection, CancellationToken cancellationToken = default)
        {
            await ExecuteNonQueryAsync(migrationCommands.ToList(), connection, new MigrationExecutionState(), commitTransaction: true, System.Data.IsolationLevel.Unspecified, cancellationToken).ConfigureAwait(false);
        }

        public async Task<int> ExecuteNonQueryAsync(IReadOnlyList<MigrationCommand> migrationCommands, IRelationalConnection connection, MigrationExecutionState executionState, bool commitTransaction, System.Data.IsolationLevel? isolationLevel = null, CancellationToken cancellationToken = default)
        {
            GaxPreconditions.CheckArgument(connection is SpannerRelationalConnection, nameof(connection), "Can only be used with Spanner connections");
            
            var inUserTransaction = connection.CurrentTransaction is not null && executionState.Transaction == null;
            if (inUserTransaction
                && (migrationCommands.Any(x => x.TransactionSuppressed) || executionStrategy.RetriesOnFailure))
            {
                throw new NotSupportedException("Cannot execute transaction suppressed migration commands in user transaction.");
            }

            using var transactionScope = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);

            return await executionStrategy.ExecuteAsync(
                (migrationCommands, connection, inUserTransaction, commitTransaction, isolationLevel),
                static (_, s, ct) => ExecuteInternalAsync(
                    s.migrationCommands,
                    s.connection,
                    beginTransaction: !s.inUserTransaction,
                    commitTransaction: !s.inUserTransaction && s.commitTransaction,
                    s.isolationLevel,
                    ct),
                verifySucceeded: null,
                cancellationToken).ConfigureAwait(false);
        }

        private static async Task<int> ExecuteInternalAsync(
            IReadOnlyList<MigrationCommand> migrationCommands,
            IRelationalConnection connection,
            bool beginTransaction,
            bool commitTransaction,
            System.Data.IsolationLevel? isolationLevel,
            CancellationToken cancellationToken)
        {
            var result = 0;
            var connectionOpened = await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            
            try
            {
                var statements = migrationCommands.Select(x => x.CommandText).ToArray();
                if (statements.Length == 0)
                {
                    return result;
                }
                
                var ddlStatements = statements.Where(IsDdlStatement).ToArray();
                var otherStatements = statements.Where(x => !IsDdlStatement(x)).ToArray();
                var spannerConnection = ((SpannerRelationalConnection)connection).DbConnection as SpannerRetriableConnection;
                
                if (ddlStatements.Any())
                {
                    var cmd = spannerConnection.CreateDdlCommand(ddlStatements[0], ddlStatements.Skip(1).ToArray());
                    result += await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                
                if (otherStatements.Any())
                {
                    using var transaction = beginTransaction ? await spannerConnection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false) : null;

                    var cmd = spannerConnection.CreateBatchDmlCommand();

                    if (transaction != null)
                    {
                        cmd.Transaction = transaction;
                    }

                    foreach (var statement in otherStatements)
                    {
                        cmd.Add(statement);
                    }
                    // Batch DML returns IReadOnlyList<long>, so sum the update counts
                    var updateCounts = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    result += (int)updateCounts.Sum();

                    if (commitTransaction && transaction != null)
                    {
                        // Commit the transaction if required
                        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch
            {
                await connection.CloseAsync().ConfigureAwait(false);
                throw;
            }

            await connection.CloseAsync().ConfigureAwait(false);
            return result;
        }

        private static bool IsDdlStatement(string statement)
        {
            return SpannerCommandTextBuilder.FromCommandText(statement).SpannerCommandType == SpannerCommandType.Ddl;
        }
    }
}
