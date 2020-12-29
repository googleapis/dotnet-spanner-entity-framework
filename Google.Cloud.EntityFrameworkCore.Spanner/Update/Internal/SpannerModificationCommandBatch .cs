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
using Google.Cloud.Spanner.Data;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
    internal class SpannerPendingCommitTimestampColumnModification : ColumnModification
    {
        internal const string PendingCommitTimestamp = "PENDING_COMMIT_TIMESTAMP()";

        internal SpannerPendingCommitTimestampColumnModification(ColumnModification modification, bool sensitiveLoggingEnabled)
            : base(modification.Entry, modification.Property, () => "", modification.IsRead, modification.IsWrite, modification.IsKey, modification.IsCondition, modification.IsConcurrencyToken, sensitiveLoggingEnabled)
        {
        }

        public override bool IsWrite => true;

        public override bool UseCurrentValueParameter => false;

        public override bool UseOriginalValueParameter => false;

        public override object Value { get => PendingCommitTimestamp; set => base.Value = value; }
    }


    internal class SpannerModificationCommand : ModificationCommand
    {
        private readonly ModificationCommand _delegate;
        private readonly IReadOnlyList<ColumnModification> _columnModifications;

        internal SpannerModificationCommand(ModificationCommand cmd, bool sensitiveLoggingEnabled) : base(cmd.TableName, cmd.Schema, cmd.ColumnModifications, sensitiveLoggingEnabled)
        {
            _delegate = cmd;
            List<ColumnModification> columnModifications = new List<ColumnModification>(cmd.ColumnModifications.Count);
            foreach (ColumnModification columnModification in cmd.ColumnModifications)
            {
                if (IsCommitTimestampModification(columnModification))
                {
                    columnModifications.Add(new SpannerPendingCommitTimestampColumnModification(columnModification, sensitiveLoggingEnabled));
                }
                else
                {
                    columnModifications.Add(columnModification);
                }
            }
            _columnModifications = columnModifications.AsReadOnly();
        }

        private bool IsCommitTimestampModification(ColumnModification columnModification)
        {
            if (columnModification.Property.FindAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp) != null)
            {
                if (columnModification.Property.FindAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp).Value is SpannerUpdateCommitTimestamp updateCommitTimestamp)
                {
                    switch (updateCommitTimestamp)
                    {
                        case SpannerUpdateCommitTimestamp.OnInsert:
                            return columnModification.Entry.EntityState == EntityState.Added;
                        case SpannerUpdateCommitTimestamp.OnUpdate:
                            return columnModification.Entry.EntityState == EntityState.Modified;
                        case SpannerUpdateCommitTimestamp.OnInsertAndUpdate:
                            return columnModification.Entry.EntityState == EntityState.Added || columnModification.Entry.EntityState == EntityState.Modified;
                        case SpannerUpdateCommitTimestamp.Never:
                        default:
                            return false;
                    }
                }
            }
            return false;
        }

        public override IReadOnlyList<ColumnModification> ColumnModifications { get => _columnModifications; }

        public override EntityState EntityState => _delegate.EntityState;
    }

    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerModificationCommandBatch : ModificationCommandBatch
    {
        private readonly IRelationalTypeMappingSource _typeMapper;
        private readonly List<ModificationCommand> _modificationCommands = new List<ModificationCommand>();
        private readonly string[] _statementTerminator;

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public SpannerModificationCommandBatch(
            [NotNull] ModificationCommandBatchFactoryDependencies dependencies,
            IRelationalTypeMappingSource typeMapper)
        {
            Dependencies = dependencies;
            _typeMapper = typeMapper;
            _statementTerminator = new string[] { dependencies.SqlGenerationHelper.StatementTerminator };
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
            if (HasCommitTimestampColumn(modificationCommand))
            {
                // _modificationCommands.Add(modificationCommand);
                _modificationCommands.Add(new SpannerModificationCommand(modificationCommand, Dependencies.Logger.ShouldLogSensitiveData()));
            }
            else
            {
                _modificationCommands.Add(modificationCommand);
            }
            return true;
        }

        private bool HasCommitTimestampColumn(ModificationCommand modificationCommand)
        {
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
            var spannerConnection = (SpannerConnection)connection.DbConnection;
            var transaction = connection.CurrentTransaction?.GetDbTransaction() as SpannerTransaction;
            if (transaction == null)
            {
                throw new InvalidOperationException("There is no active transaction");
            }
            var cmd = CreateSpannerBatchDmlCommand(spannerConnection, transaction);
            await cmd.Item1.ExecuteNonQueryAsync(cancellationToken);
            if (cmd.Item2.Count > 0)
            {
                await PropagateResults(cmd.Item2, cancellationToken);
            }
        }

        private async Task PropagateResults(List<SpannerCommand> selectCommands, CancellationToken cancellationToken)
        {
            int index = 0;
            foreach (var modificationCommand in _modificationCommands)
            {
                if (modificationCommand.RequiresResultPropagation)
                {
                    using (var reader = await selectCommands[index].ExecuteReaderAsync(cancellationToken))
                    {
                        if (await reader.ReadAsync(cancellationToken))
                        {
                            var valueBufferFactory = CreateValueBufferFactory(modificationCommand.ColumnModifications);
                            modificationCommand.PropagateResults(valueBufferFactory.Create(reader));
                        }
                        index++;
                    }
                }
            }
        }

        private Tuple<SpannerBatchCommand, List<SpannerCommand>> CreateSpannerBatchDmlCommand(SpannerConnection connection, SpannerTransaction transaction)
        {
            var selectCommands = new List<SpannerCommand>();
            var commandPosition = 0;
            var cmd = transaction.CreateBatchDmlCommand();
            cmd.CommandType = SpannerBatchCommandType.BatchDml;
            foreach (var modificationCommand in _modificationCommands)
            {
                var commands = CreateSpannerCommand(connection, transaction, modificationCommand, commandPosition);
                cmd.Add(commands.Item1);
                if (commands.Item2 != null)
                {
                    selectCommands.Add(commands.Item2);
                }
                commandPosition++;
            }
            return Tuple.Create(cmd, selectCommands);
        }

        private Tuple<SpannerCommand, SpannerCommand> CreateSpannerCommand(SpannerConnection connection, SpannerTransaction transaction, ModificationCommand modificationCommand, int commandPosition)
        {
            var builder = new StringBuilder();
            ResultSetMapping res;
            IEnumerable<ColumnModification> parameterColumns;
            switch (modificationCommand.EntityState)
            {
                case EntityState.Deleted:
                    res = Dependencies.UpdateSqlGenerator.AppendDeleteOperation(builder, modificationCommand, commandPosition);
                    parameterColumns = modificationCommand.ColumnModifications.Where(o => o.IsCondition);
                    break;
                case EntityState.Modified:
                    res = Dependencies.UpdateSqlGenerator.AppendUpdateOperation(builder, modificationCommand, commandPosition);
                    parameterColumns = modificationCommand.ColumnModifications.Where(o => o.IsWrite || o.IsCondition);
                    break;
                case EntityState.Added:
                    res = Dependencies.UpdateSqlGenerator.AppendInsertOperation(builder, modificationCommand, commandPosition);
                    parameterColumns = modificationCommand.ColumnModifications.Where(o => o.IsWrite);
                    break;
                default:
                    throw new NotSupportedException(
                        $"Modification type {modificationCommand.EntityState} is not supported.");
            }
            string dml;
            SpannerCommand selectCommand = null;
            if (res != ResultSetMapping.NoResultSet)
            {
                var commandTexts = builder.ToString().Split(_statementTerminator, StringSplitOptions.RemoveEmptyEntries);
                dml = commandTexts[0];
                if (commandTexts.Length > 1)
                {
                    selectCommand = connection.CreateSelectCommand(commandTexts[1]);
                    selectCommand.Transaction = transaction;
                    var selectParamIndex = 0;
                    foreach (var columnModification in modificationCommand.ColumnModifications)
                    {
                        if (columnModification.IsKey && (columnModification.UseOriginalValueParameter || columnModification.UseCurrentValueParameter))
                        {
                            selectCommand.Parameters.Add(
                                _typeMapper.GetMapping(columnModification.Property).CreateParameter(selectCommand,
                                    $"@p{selectParamIndex}",
                                    columnModification.UseOriginalValueParameter
                                        ? columnModification.OriginalValue
                                        : columnModification.Value, columnModification.Property.IsNullable));
                            selectParamIndex++;
                        }
                    }
                }
            }
            else
            {
                dml = builder.ToString();
            }
            var cmd = connection.CreateDmlCommand(dml);
            var paramIndex = 0;
            foreach (var columnModification in modificationCommand.ColumnModifications.Where(o => o.UseOriginalValueParameter || o.UseCurrentValueParameter))
            {
                cmd.Parameters.Add(
                    _typeMapper.GetMapping(columnModification.Property).CreateParameter(cmd,
                        columnModification.UseOriginalValueParameter
                            ? columnModification.OriginalParameterName
                            : columnModification.ParameterName,
                        columnModification.UseOriginalValueParameter
                            ? columnModification.OriginalValue
                            : columnModification.Value, columnModification.Property.IsNullable));
                paramIndex++;
            }
            return Tuple.Create(cmd, selectCommand);
        }
    }
}
