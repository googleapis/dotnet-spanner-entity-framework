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
    /// </summary>
    public class SpannerModificationCommandBatch : ModificationCommandBatch
    {
        private readonly IRelationalTypeMappingSource _typeMapper;
        private readonly List<ModificationCommand> _modificationCommands = new List<ModificationCommand>();
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
        ///     Service dependencies.
        /// </summary>
        public virtual ModificationCommandBatchFactoryDependencies Dependencies { get; }

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override IReadOnlyList<ModificationCommand> ModificationCommands => _modificationCommands;

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
        protected virtual IRelationalValueBufferFactory CreateValueBufferFactory(
            [NotNull] IReadOnlyList<ColumnModification> columnModifications)
            => Dependencies.ValueBufferFactoryFactory
                .Create(
                    columnModifications
                        .Where(c => c.IsRead)
                        .Select(c => new TypeMaterializationInfo(c.Property.ClrType, c.Property, _typeMapper))
                        .ToArray());

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public override bool AddCommand(ModificationCommand modificationCommand)
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
            CancellationToken cancellationToken = new CancellationToken())
        {
            var spannerRelationalConnection = (SpannerRelationalConnection)connection;
            var spannerConnection = (SpannerRetriableConnection)connection.DbConnection;
            if (!(connection.CurrentTransaction?.GetDbTransaction() is SpannerRetriableTransaction transaction))
            {
                throw new InvalidOperationException("There is no active transaction");
            }
            var useMutations = spannerRelationalConnection.MutationUsage == Infrastructure.MutationUsage.Always
                || (!_hasExplicitTransaction && spannerRelationalConnection.MutationUsage == Infrastructure.MutationUsage.ImplicitTransactions);
            if (useMutations)
            {
                int index = 0;
                foreach (var modificationCommand in _modificationCommands)
                {
                    if (modificationCommand is SpannerPendingCommitTimestampModificationCommand commitTimestampModificationCommand)
                    {
                        commitTimestampModificationCommand.MarkAsMutationCommand();
                    }
                    var cmd = CreateSpannerMutationCommand(spannerConnection, transaction, modificationCommand);
                    // Note: The following line does not actually execute any command on the backend, it only buffers
                    // the mutation locally to be sent with the next Commit statement.
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                    // We assume that each mutation will affect exactly one row. This assumption always holds for INSERT
                    // and UPDATE mutations (unless they return an error). DELETE mutations could affect zero rows if the
                    // row had already been deleted, and more than one row if the deleted row is in a table with one or
                    // more INTERLEAVED tables that are defined with ON DELETE CASCADE.
                    UpdateCounts.Add(1L);
                    // Check whether we need to generate a SELECT command to propagate computed values back to the context.
                    if (modificationCommand.RequiresResultPropagation)
                    {
                        var builder = new StringBuilder();
                        var operations = modificationCommand.ColumnModifications;
                        var keyOperations = operations.Where(o => o.IsKey).ToList();
                        var readOperations = operations.Where(o => o.IsRead).ToList();
                        var sql = ((SpannerUpdateSqlGenerator)Dependencies.UpdateSqlGenerator).GenerateSelectAffectedSql(
                            modificationCommand.TableName, modificationCommand.Schema, readOperations, keyOperations, index);
                        _propagateResultsCommands.Add(CreateSelectedAffectedCommand(spannerConnection, modificationCommand, sql));
                    }
                    index++;
                }
            }
            else
            {
                var cmd = CreateSpannerBatchDmlCommand(spannerConnection, transaction);
                UpdateCounts = (await cmd.Item1.ExecuteNonQueryAsync(cancellationToken)).ToList();
                if (RowsAffected != _modificationCommands.Count)
                {
                    ThrowAggregateUpdateConcurrencyException();
                }
                if (cmd.Item2.Count > 0)
                {
                    _propagateResultsCommands.AddRange(cmd.Item2);
                }
            }
        }

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

        internal async Task PropagateResults(CancellationToken cancellationToken)
        {
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

        private Tuple<SpannerRetriableBatchCommand, List<SpannerRetriableCommand>> CreateSpannerBatchDmlCommand(SpannerRetriableConnection connection, SpannerRetriableTransaction transaction)
        {
            var selectCommands = new List<SpannerRetriableCommand>();
            var commandPosition = 0;
            var cmd = connection.CreateBatchDmlCommand();
            cmd.Transaction = transaction;
            foreach (var modificationCommand in _modificationCommands)
            {
                var commands = CreateSpannerDmlCommand(connection, transaction, modificationCommand, commandPosition);
                cmd.Add(commands.Item1);
                if (commands.Item2 != null)
                {
                    selectCommands.Add(commands.Item2);
                }
                commandPosition++;
            }
            return Tuple.Create(cmd, selectCommands);
        }

        private Tuple<SpannerCommand, SpannerRetriableCommand> CreateSpannerDmlCommand(SpannerRetriableConnection connection, SpannerRetriableTransaction transaction, ModificationCommand modificationCommand, int commandPosition)
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
            AppendWriteParameters(modificationCommand, cmd, false);
            return Tuple.Create(cmd, selectCommand);
        }

        private SpannerCommand CreateSpannerMutationCommand(
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
            cmd.Transaction = transaction.SpannerTransaction;
            AppendWriteParameters(modificationCommand, cmd, true);
            return cmd;
        }

        private void AppendWriteParameters(ModificationCommand modificationCommand, SpannerCommand cmd, bool useColumnName)
        {
            foreach (var columnModification in modificationCommand.ColumnModifications.Where(o => o.UseOriginalValueParameter))
            {
                cmd.Parameters.Add(CreateParameter(columnModification, cmd, UseValue.Original, useColumnName));
            }
            foreach (var columnModification in modificationCommand.ColumnModifications.Where(o => o.UseCurrentValueParameter))
            {
                cmd.Parameters.Add(CreateParameter(columnModification, cmd, UseValue.Current, useColumnName));
            }
        }

        private SpannerRetriableCommand CreateSelectedAffectedCommand(SpannerRetriableConnection connection, ModificationCommand modificationCommand, string sql)
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

        private DbParameter CreateParameter(ColumnModification columnModification, DbCommand cmd, UseValue useValue, bool useColumnName)
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
