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
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// 
    /// SpannerModificationCommandBatch will by default execute updates using mutations when implicit
    /// transactions are being used, and DML when explicit transactions are being used. Using DML for
    /// explicit transactions allows the transaction to read its own writes. Using mutations for implicit
    /// transactions is more efficient, as mutations are faster than DML and implicit transactions imply
    /// that read-your-writes is not needed.
    /// 
    /// Client applications can configure mutation usage when creating a DbContext to change the default
    /// behavior:
    /// * MutationUsage.ImplicitTransactions (default): Use mutations when implicit transactions are used.
    /// * Never: Never use mutations and always use DML. This will reduce the performance of implicit transactions.
    /// * Always: Always use mutations, also for explicit transactions. This will break read-your-writes in transactions.
    /// </summary>
    internal sealed class SpannerModificationCommandBatch : ModificationCommandBatch
    {
        private readonly IRelationalTypeMappingSource _typeMapper;
        private readonly List<IReadOnlyModificationCommand> _modificationCommands = new List<IReadOnlyModificationCommand>();
        private readonly List<SpannerRetriableCommand> _propagateResultsCommands = new List<SpannerRetriableCommand>();
        private readonly char _statementTerminator;
        private readonly bool _hasExplicitTransaction;

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public SpannerModificationCommandBatch(
            [NotNull] ModificationCommandBatchFactoryDependencies dependencies,
            IRelationalTypeMappingSource typeMapper)
        {
            Dependencies = dependencies;
            _typeMapper = typeMapper;
            // This class needs a statement terminator because the EFCore built-in SQL generator helper
            // will generate multiple statements as one string.
            _statementTerminator = ';';
             _hasExplicitTransaction = dependencies.CurrentContext.Context.Database.CurrentTransaction != null;
        }

        /// <summary>
        /// Service dependencies.
        /// </summary>
        public ModificationCommandBatchFactoryDependencies Dependencies { get; }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override IReadOnlyList<IReadOnlyModificationCommand> ModificationCommands => _modificationCommands;

        /// <summary>
        /// The affected rows per modification command in this batch. This property is only valid after the batch has been executed.
        /// </summary>
        internal List<long> UpdateCounts { get; private set; } = new List<long>();

        internal int RowsAffected
        {
            // This ensures that the deletion of a record in a parent table that also cascades to
            // one or more child tables is still only counted as one mutation.
            get => UpdateCounts.Select(c => c == 0L ? 0 : 1).Sum();
        }

        /// <summary>
        ///     Creates the <see cref="IRelationalValueBufferFactory" /> that will be used for creating a
        ///     <see cref="ValueBuffer" /> to consume the data reader.
        /// </summary>
        /// <param name="columnModifications">
        ///     The list of <see cref="ColumnModification" />s for all the columns
        ///     being modified such that a ValueBuffer with appropriate slots can be created.
        /// </param>
        /// <returns> The factory. </returns>
        private IRelationalValueBufferFactory CreateValueBufferFactory(
            [NotNull] IReadOnlyList<IColumnModification> columnModifications)
            => Dependencies.ValueBufferFactoryFactory
                .Create(
                    columnModifications
                        .Where(c => c.IsRead)
                        .Select(c => new TypeMaterializationInfo(c.Property.ClrType, c.Property, _typeMapper.FindMapping(c.Property)))
                        .ToArray());

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override bool AddCommand(IReadOnlyModificationCommand modificationCommand)
        {
            if (SpannerPendingCommitTimestampModificationCommand.HasCommitTimestampColumn(modificationCommand))
            {
                _modificationCommands.Add(new SpannerPendingCommitTimestampModificationCommand(modificationCommand, Dependencies.Logger.ShouldLogSensitiveData()));
            }
            else
            {
                _modificationCommands.Add(modificationCommand);
            }
            return true;
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override void Execute(IRelationalConnection connection)
        {
            Task.Run(() => ExecuteAsync(connection)).WaitWithUnwrappedExceptions();
        }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override async Task ExecuteAsync(IRelationalConnection connection,
            CancellationToken cancellationToken = default)
        {
            var spannerRelationalConnection = (SpannerRelationalConnection)connection;
            var spannerConnection = (SpannerRetriableConnection)connection.DbConnection;
            // There should always be a transaction:
            // 1. Implicit: A transaction is automatically started by Entity Framework when SaveChanges() is called.
            // 2. Explicit: The client application has called BeginTransaction() on the database.
            if (!(connection.CurrentTransaction?.GetDbTransaction() is SpannerRetriableTransaction transaction))
            {
                throw new InvalidOperationException("There is no active transaction. Cloud Spanner does not support executing updates without a transaction.");
            }
            var useMutations = spannerRelationalConnection.MutationUsage == Infrastructure.MutationUsage.Always
                || (!_hasExplicitTransaction && spannerRelationalConnection.MutationUsage == Infrastructure.MutationUsage.ImplicitTransactions);
            if (useMutations)
            {
                await ExecuteMutationsAsync(spannerConnection, transaction, cancellationToken);
            }
            else
            {
                await ExecuteDmlAsync(spannerConnection, transaction, cancellationToken);
            }
        }

        /// <summary>
        /// Executes the command batch using mutations. Mutations are more efficient than DML, but do not support read-your-writes.
        /// Mutations are therefore by default only used for implicit transactions, but client applications can configure the Spanner
        /// Entity Framework provider to use mutations for explicit transactions as well.
        /// </summary>
        private async Task ExecuteMutationsAsync(
            SpannerRetriableConnection spannerConnection, SpannerRetriableTransaction transaction, CancellationToken cancellationToken)
        {
            int index = 0;
            foreach (var modificationCommand in _modificationCommands)
            {
                // We assume that each mutation will affect exactly one row. This assumption always holds for INSERT
                // and UPDATE mutations (unless they return an error). DELETE mutations could affect zero rows if the
                // row had already been deleted, and more than one row if the deleted row is in a table with one or
                // more INTERLEAVED tables that are defined with ON DELETE CASCADE.
                //
                // This can be changed if a concurrency token check fails.
                var updateCount = 1L;

                // Concurrency token checks cannot be included in mutations. Instead, we need to do manual select to check
                // that the concurrency token is still the same as what we expect. This select is executed in the same
                // transaction as the mutations, so it is guaranteed that the value that we read here will still be valid
                // when the mutations are committed.
                var operations = modificationCommand.ColumnModifications;
                var hasConcurrencyCondition = operations.Any(o => o.IsCondition && (o.Property?.IsConcurrencyToken ?? false));
                if (hasConcurrencyCondition)
                {
                    var conditionOperations = operations.Where(o => o.IsCondition).ToList();
                    var concurrencySql = ((SpannerUpdateSqlGenerator)Dependencies.UpdateSqlGenerator).GenerateSelectConcurrencyCheckSql(modificationCommand.TableName, conditionOperations);
                    var concurrencyCommand = spannerConnection.CreateSelectCommand(concurrencySql);
                    concurrencyCommand.Transaction = transaction;
                    foreach (var columnModification in conditionOperations)
                    {
                        concurrencyCommand.Parameters.Add(CreateParameter(columnModification, concurrencyCommand, UseValue.Original, false));
                    }
                    // Execute the concurrency check query in the read/write transaction and check whether the expected row exists.
                    using var reader = await concurrencyCommand.ExecuteReaderAsync(cancellationToken);
                    if (!await reader.ReadAsync(cancellationToken))
                    {
                        // Set the update count to 0 to trigger a concurrency exception.
                        // We do not throw the exception here already, as there might be more concurrency problems,
                        // and we want to be able to report all in the exception.
                        updateCount = 0L;
                    }
                }

                // Mutation commands must use a specific TIMESTAMP constant for pending commit timestamps instead of the
                // placeholder string PENDING_COMMIT_TIMESTAMP(). This instructs any pending commit timestamp modifications
                // to use the mutation constant instead.
                if (modificationCommand is SpannerPendingCommitTimestampModificationCommand commitTimestampModificationCommand)
                {
                    commitTimestampModificationCommand.MarkAsMutationCommand();
                    transaction.AddSpannerPendingCommitTimestampModificationCommand(commitTimestampModificationCommand);
                }
                // Create the mutation command and execute it.
                var cmd = CreateSpannerMutationCommand(spannerConnection, transaction, modificationCommand);
                // Note: The following line does not actually execute any command on the backend, it only buffers
                // the mutation locally to be sent with the next Commit statement.
                await cmd.ExecuteNonQueryAsync(cancellationToken);
                UpdateCounts.Add(updateCount);

                // Check whether we need to generate a SELECT command to propagate computed values back to the context.
                // This SELECT command will be executed outside of the current implicit transaction.
                // The propagation query is skipped if the batch uses an explicit transaction, as it will not be able
                // to read the new value anyways.
                if (modificationCommand.RequiresResultPropagation && !_hasExplicitTransaction)
                {
                    var keyOperations = operations.Where(o => o.IsKey).ToList();
                    var readOperations = operations.Where(o => o.IsRead).ToList();
                    var sql = ((SpannerUpdateSqlGenerator)Dependencies.UpdateSqlGenerator).GenerateSelectAffectedSql(
                        modificationCommand.TableName, modificationCommand.Schema, readOperations, keyOperations, index);
                    _propagateResultsCommands.Add(CreateSelectedAffectedCommand(spannerConnection, modificationCommand, sql));
                }
                index++;
            }
            // Check that there were no concurrency problems detected.
            if (RowsAffected != _modificationCommands.Count)
            {
                ThrowAggregateUpdateConcurrencyException();
            }
        }

        /// <summary>
        /// Executes the command batch using DML. DML is less efficient than mutations, but do allow applications
        /// to read their own writes within a transaction. DML is therefore used by default for explicit transactions.
        /// Applications can also configure the Spanner Entity Framework provider to use DML for implicit transactions as well.
        /// </summary>
        private async Task ExecuteDmlAsync(SpannerRetriableConnection spannerConnection, SpannerRetriableTransaction transaction, CancellationToken cancellationToken)
        {
            // Create a Batch DML command that contains all the updates in this batch.
            // The update statements will include any concurrency token checks that might be needed.
            var cmd = CreateSpannerBatchDmlCommand(spannerConnection, transaction);
            UpdateCounts = (await cmd.Item1.ExecuteNonQueryAsync(cancellationToken)).ToList();
            if (RowsAffected != _modificationCommands.Count)
            {
                ThrowAggregateUpdateConcurrencyException();
            }
            // Add any select commands that were generated by the batch for updates that need to propagate results.
            if (cmd.Item2.Count > 0)
            {
                _propagateResultsCommands.AddRange(cmd.Item2);
            }
        }

        /// <summary>
        /// Constructs and throws a DbUpdateConcurrencyException for this batch based on the UpdateCounts.
        /// </summary>
        private void ThrowAggregateUpdateConcurrencyException()
        {
            var expectedRowsAffected = _modificationCommands.Count;
            var index = 0;
            var entries = new List<IUpdateEntry>();
            foreach (var c in UpdateCounts)
            {
                if (c == 0L)
                {
                    entries.AddRange(ModificationCommands[index].Entries);
                }
                index++;
            }

            throw new DbUpdateConcurrencyException(
                RelationalStrings.UpdateConcurrencyException(expectedRowsAffected, RowsAffected), entries);
        }

        internal void PropagateResults()
        {
            if (_propagateResultsCommands.Count == 0)
            {
                return;
            }
            Task.Run(() => PropagateResultsAsync()).WaitWithUnwrappedExceptions();
        }

        /// <summary>
        /// Propagates results from update statements that caused a computed column to be changed.
        /// Result propagation is done by executing a separate SELECT statement on the table that was
        /// updated. These SELECT statements are executed after the batch has been executed, and outside
        /// of the transaction if implicit transactions are used.
        /// 
        /// If the batch uses an explicit transaction, the result propagation will be executed inside the
        /// transaction.
        /// </summary>
        internal async Task PropagateResultsAsync(CancellationToken cancellationToken = default)
        {
            if (_propagateResultsCommands.Count == 0)
            {
                return;
            }
            int index = 0;
            foreach (var modificationCommand in _modificationCommands)
            {
                if (modificationCommand.RequiresResultPropagation)
                {
                    using var reader = await _propagateResultsCommands[index].ExecuteReaderAsync(cancellationToken);
                    if (await reader.ReadAsync(cancellationToken))
                    {
                        var valueBufferFactory = CreateValueBufferFactory(modificationCommand.ColumnModifications);
                        modificationCommand.PropagateResults(valueBufferFactory.Create(reader));
                    }
                    index++;
                }
            }
        }

        /// <summary>
        /// Generates a Batch DML command for the modifications in this batch and SELECT statements for any
        /// results that need to be propagated after the update.
        /// </summary>
        private Tuple<SpannerRetriableBatchCommand, List<SpannerRetriableCommand>> CreateSpannerBatchDmlCommand(SpannerRetriableConnection connection, SpannerRetriableTransaction transaction)
        {
            var selectCommands = new List<SpannerRetriableCommand>();
            var commandPosition = 0;
            var cmd = connection.CreateBatchDmlCommand();
            cmd.Transaction = transaction;
            foreach (var modificationCommand in _modificationCommands)
            {
                var commands = CreateSpannerDmlCommand(connection, modificationCommand, commandPosition);
                cmd.Add(commands.Item1);
                if (commands.Item2 != null)
                {
                    if (_hasExplicitTransaction)
                    {
                        commands.Item2.Transaction = transaction;
                    }
                    selectCommands.Add(commands.Item2);
                }
                if (modificationCommand is SpannerPendingCommitTimestampModificationCommand commitTimestampModificationCommand)
                {
                    transaction.AddSpannerPendingCommitTimestampModificationCommand(commitTimestampModificationCommand);
                }
                commandPosition++;
            }
            return Tuple.Create(cmd, selectCommands);
        }

        private Tuple<SpannerCommand, SpannerRetriableCommand> CreateSpannerDmlCommand(SpannerRetriableConnection connection, IReadOnlyModificationCommand modificationCommand, int commandPosition)
        {
            var builder = new StringBuilder();
            ResultSetMapping res;
            switch (modificationCommand.EntityState)
            {
                case EntityState.Deleted:
                    res = Dependencies.UpdateSqlGenerator.AppendDeleteOperation(builder, modificationCommand, commandPosition);
                    break;
                case EntityState.Modified:
                    res = Dependencies.UpdateSqlGenerator.AppendUpdateOperation(builder, modificationCommand, commandPosition);
                    break;
                case EntityState.Added:
                    res = Dependencies.UpdateSqlGenerator.AppendInsertOperation(builder, modificationCommand, commandPosition);
                    break;
                default:
                    throw new NotSupportedException(
                        $"Modification type {modificationCommand.EntityState} is not supported.");
            }
            string dml;
            SpannerRetriableCommand selectCommand = null;
            if (res != ResultSetMapping.NoResultSet)
            {
                var commandTexts = builder.ToString().Split(_statementTerminator);
                dml = commandTexts[0];
                if (commandTexts.Length > 1)
                {
                    selectCommand = CreateSelectedAffectedCommand(connection, modificationCommand, commandTexts[1]);
                }
            }
            else
            {
                dml = builder.ToString();
                dml = dml.TrimEnd('\r', '\n', _statementTerminator);
            }
            // This intentionally uses a SpannerCommand instead of the internal SpannerRetriableCommand, because the command
            // will eventually be added to a BatchCommand.
            var cmd = connection.SpannerConnection.CreateDmlCommand(dml);
            AppendWriteParameters(modificationCommand, cmd, false, true);
            return Tuple.Create(cmd, selectCommand);
        }

        private SpannerRetriableCommand CreateSpannerMutationCommand(
            SpannerRetriableConnection spannerConnection,
            SpannerRetriableTransaction transaction,
            IReadOnlyModificationCommand modificationCommand)
        {
            var cmd = modificationCommand.EntityState switch
            {
                EntityState.Deleted => spannerConnection.CreateDeleteCommand(modificationCommand.TableName),
                EntityState.Modified => spannerConnection.CreateUpdateCommand(modificationCommand.TableName),
                EntityState.Added => spannerConnection.CreateInsertCommand(modificationCommand.TableName),
                _ => throw new NotSupportedException($"Modification type {modificationCommand.EntityState} is not supported."),
            };
            cmd.Transaction = transaction;
            AppendWriteParameters(modificationCommand, cmd, true, false);
            return cmd;
        }

        /// <summary>
        /// Adds the parameters that need to be written for an update command. This can be both a DML and a mutation command.
        /// 
        /// ConcurrencyToken conditions are not included in mutation commands, as these do not support a WHERE clause or other filtering.
        /// </summary>
        private void AppendWriteParameters(IReadOnlyModificationCommand modificationCommand, DbCommand cmd, bool useColumnName, bool includeConcurrencyTokenConditions)
        {
            foreach (var columnModification in modificationCommand.ColumnModifications.Where(
                o => o.UseOriginalValueParameter && (includeConcurrencyTokenConditions || !(o.IsCondition && (o.Property?.IsConcurrencyToken ?? false))))
                         .OrderBy(m => m.Property!.GetIndex()))
            {
                cmd.Parameters.Add(CreateParameter(columnModification, cmd, UseValue.Original, useColumnName));
            }
            foreach (var columnModification in modificationCommand.ColumnModifications.Where(o => o.UseCurrentValueParameter)
                         .OrderBy(m => m.Property!.GetIndex()))
            {
                cmd.Parameters.Add(CreateParameter(columnModification, cmd, UseValue.Current, useColumnName));
            }
        }

        /// <summary>
        /// Creates a SELECT command for a result that needs to be propagated after the update.
        /// </summary>
        private SpannerRetriableCommand CreateSelectedAffectedCommand(SpannerRetriableConnection connection, IReadOnlyModificationCommand modificationCommand, string sql)
        {
            var selectCommand = connection.CreateSelectCommand(sql);
            foreach (var columnModification in modificationCommand.ColumnModifications)
            {
                if (columnModification.IsKey && (columnModification.UseOriginalValueParameter || columnModification.UseCurrentValueParameter))
                {
                    selectCommand.Parameters.Add(CreateParameter(columnModification, selectCommand, columnModification.UseOriginalValueParameter ? UseValue.Original : UseValue.Current, false));
                }
            }
            return selectCommand;
        }

        /// <summary>
        /// Creates a SpannerParameter for a command and sets the correct type.
        /// </summary>
        private DbParameter CreateParameter(IColumnModification columnModification, DbCommand cmd, UseValue useValue, bool useColumnName)
        {
            string paramName;
            if (useColumnName)
            {
                paramName = columnModification.ColumnName;
            }
            else
            {
                paramName = useValue == UseValue.Original ? columnModification.OriginalParameterName : columnModification.ParameterName;
            }
            var param = _typeMapper.GetMapping(columnModification.Property).CreateParameter(cmd,
                paramName,
                useValue == UseValue.Original ? columnModification.OriginalValue : columnModification.Value,
                columnModification.Property.IsNullable);
            if (param is SpannerParameter spannerParameter && SpannerDbType.Unspecified.Equals(spannerParameter.SpannerDbType))
            {
                spannerParameter.SpannerDbType = SpannerDbType.FromClrType(GetUnderlyingTypeOrSelf(columnModification.Property.ClrType));
            }
            return param;
        }

        private System.Type GetUnderlyingTypeOrSelf(System.Type type)
        {
            var underlying = Nullable.GetUnderlyingType(type);
            return underlying == null ? type : underlying;
        }
    }

    enum UseValue
    {
        Original,
        Current
    }
}
