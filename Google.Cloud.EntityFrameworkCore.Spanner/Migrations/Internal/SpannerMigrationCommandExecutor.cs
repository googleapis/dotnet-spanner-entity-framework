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

            using var transactionScope = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);

            return executionStrategy.Execute(
                (migrationCommands, connection, executionState, inUserTransaction, commitTransaction, isolationLevel),
                static (_, s) => Execute(
                    s.migrationCommands,
                    s.connection,
                    s.executionState,
                    beginTransaction: !s.inUserTransaction,
                    commitTransaction: !s.inUserTransaction && s.commitTransaction,
                    s.isolationLevel),
                verifySucceeded: null);
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
                (migrationCommands, connection, executionState, inUserTransaction, commitTransaction, isolationLevel),
                static (_, s, ct) => ExecuteAsync(
                    s.migrationCommands,
                    s.connection,
                    s.executionState,
                    beginTransaction: !s.inUserTransaction,
                    commitTransaction: !s.inUserTransaction && s.commitTransaction,
                    s.isolationLevel,
                    ct),
                verifySucceeded: null,
                cancellationToken).ConfigureAwait(false);
        }

        private static int Execute(
            IReadOnlyList<MigrationCommand> migrationCommands,
            IRelationalConnection connection,
            MigrationExecutionState executionState,
            bool beginTransaction,
            bool commitTransaction,
            System.Data.IsolationLevel? isolationLevel)
        {
            var result = 0;
            var connectionOpened = connection.Open();
            
            try
            {
                for (var i = executionState.LastCommittedCommandIndex; i < migrationCommands.Count; i++)
                {
                    var command = migrationCommands[i];
                    if (executionState.Transaction == null
                        && !command.TransactionSuppressed
                        && beginTransaction)
                    {
                        executionState.Transaction = isolationLevel == null
                            ? connection.BeginTransaction()
                            : connection.BeginTransaction(isolationLevel.Value);
                        if (executionState.DatabaseLock != null)
                        {
                            executionState.DatabaseLock = executionState.DatabaseLock.ReacquireIfNeeded(
                                connectionOpened, transactionRestarted: true);
                            connectionOpened = false;
                        }
                    }

                    if (executionState.Transaction != null
                        && command.TransactionSuppressed)
                    {
                        executionState.Transaction.Commit();
                        executionState.Transaction.Dispose();
                        executionState.Transaction = null;
                        executionState.LastCommittedCommandIndex = i;
                        executionState.AnyOperationPerformed = true;

                        if (executionState.DatabaseLock != null)
                        {
                            executionState.DatabaseLock = executionState.DatabaseLock.ReacquireIfNeeded(
                                connectionOpened, transactionRestarted: null);
                            connectionOpened = false;
                        }
                    }

                    result = command.ExecuteNonQuery(connection);

                    if (executionState.Transaction == null)
                    {
                        executionState.LastCommittedCommandIndex = i + 1;
                        executionState.AnyOperationPerformed = true;
                    }
                }

                if (commitTransaction
                    && executionState.Transaction != null)
                {
                    executionState.Transaction.Commit();
                    executionState.Transaction.Dispose();
                    executionState.Transaction = null;
                }
            }
            catch
            {
                executionState.Transaction?.Dispose();
                executionState.Transaction = null;
                connection.Close();
                throw;
            }

            connection.Close();
            return result;
        }

        private static async Task<int> ExecuteAsync(
            IReadOnlyList<MigrationCommand> migrationCommands,
            IRelationalConnection connection,
            MigrationExecutionState executionState,
            bool beginTransaction,
            bool commitTransaction,
            System.Data.IsolationLevel? isolationLevel,
            CancellationToken cancellationToken)
        {
            var result = 0;
            var connectionOpened = await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            var spannerConnection = ((SpannerRelationalConnection)connection).DbConnection as SpannerRetriableConnection;

            try
            {
                for (var i = executionState.LastCommittedCommandIndex; i < migrationCommands.Count; i++)
                {
                    var lockReacquired = false;
                    var command = migrationCommands[i];
                    if (executionState.Transaction == null
                        && !command.TransactionSuppressed
                        && beginTransaction)
                    {
                        executionState.Transaction = await (isolationLevel == null
                            ? connection.BeginTransactionAsync(cancellationToken)
                            : connection.BeginTransactionAsync(isolationLevel.Value, cancellationToken))
                            .ConfigureAwait(false);

                        if (executionState.DatabaseLock != null)
                        {
                            executionState.DatabaseLock = await executionState.DatabaseLock.ReacquireIfNeededAsync(
                                connectionOpened, transactionRestarted: true, cancellationToken)
                                .ConfigureAwait(false);
                            lockReacquired = true;
                        }
                    }

                    if (executionState.Transaction != null
                        && command.TransactionSuppressed)
                    {
                        await executionState.Transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                        await executionState.Transaction.DisposeAsync().ConfigureAwait(false);
                        executionState.Transaction = null;
                        executionState.LastCommittedCommandIndex = i;
                        executionState.AnyOperationPerformed = true;

                        if (executionState.DatabaseLock != null
                            && !lockReacquired)
                        {
                            executionState.DatabaseLock = await executionState.DatabaseLock.ReacquireIfNeededAsync(
                                connectionOpened, transactionRestarted: null, cancellationToken)
                                .ConfigureAwait(false);
                        }
                    }

                    var spannerCommand = IsDdlStatement(command.CommandText) ? 
                        spannerConnection.CreateDdlCommand(command.CommandText) 
                        : spannerConnection.CreateDmlCommand(command.CommandText);

                    // spannerCommand.Transaction = executionState.Transaction/
                    result = await spannerCommand.ExecuteNonQueryAsync(cancellationToken: cancellationToken)
                        .ConfigureAwait(false);

                    if (executionState.Transaction == null)
                    {
                        executionState.LastCommittedCommandIndex = i + 1;
                        executionState.AnyOperationPerformed = true;
                    }
                }

                if (commitTransaction
                    && executionState.Transaction != null)
                {
                    await executionState.Transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    await executionState.Transaction.DisposeAsync().ConfigureAwait(false);
                    executionState.Transaction = null;
                }
            }
            catch
            {
                if (executionState.Transaction != null)
                {
                    await executionState.Transaction.DisposeAsync().ConfigureAwait(false);
                    executionState.Transaction = null;
                }
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
