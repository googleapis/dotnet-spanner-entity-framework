﻿// Copyright 2020, Google Inc. All rights reserved.
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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal
{
    internal class SpannerUpdateSqlGenerator : UpdateAndSelectSqlGenerator
    {
        private readonly ISqlGenerationHelper _sqlGenerationHelper;

        public SpannerUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
            : base(dependencies)
        {
            Dependencies = dependencies;
            if (dependencies.SqlGenerationHelper is SpannerSqlGenerationHelper spannerSqlGenerationHelper)
            {
                _sqlGenerationHelper = new SpannerSqlGenerationHelper(spannerSqlGenerationHelper.Dependencies, ";");
            }
            else
            {
                _sqlGenerationHelper = dependencies.SqlGenerationHelper;
            }
        }
        
        internal new UpdateSqlGeneratorDependencies Dependencies { get; }

        protected override ISqlGenerationHelper SqlGenerationHelper { get => _sqlGenerationHelper; }

        protected override void AppendIdentityWhereCondition(StringBuilder commandStringBuilder, IColumnModification columnModification)
        {
            commandStringBuilder.Append(" TRUE ");
        }

        protected override void AppendRowsAffectedWhereCondition(StringBuilder commandStringBuilder, int expectedRowsAffected)
        {
            commandStringBuilder.Append(" TRUE ");
        }

        protected override ResultSetMapping AppendSelectAffectedCountCommand(
            StringBuilder commandStringBuilder,
            string name,
#nullable enable
            string? schema,
#nullable disable
            int commandPosition)
        {
            // Spanner returns the affected rows as part of the metadata, meaning that the affected
            // rows is not in the result set. We therefore return NoResults.
            return ResultSetMapping.NoResults;
        }

        /// <summary>
        /// Generates a SQL statement for results that were affected by a modification, and that need to be read
        /// back from the database as the value was computed.
        /// </summary>
        internal string GenerateSelectAffectedSql(
            string table,
            string schema,
            IReadOnlyList<IColumnModification> readOperations,
            IReadOnlyList<IColumnModification> conditionOperations,
            int commandPosition)
        {
            var commandStringBuilder = new StringBuilder();
            base.AppendSelectAffectedCommand(commandStringBuilder, table, schema, readOperations, conditionOperations, commandPosition);
            var sql = commandStringBuilder.ToString().Trim();
            if (sql.EndsWith(_sqlGenerationHelper.StatementTerminator))
            {
                sql = sql.Substring(0, sql.Length - _sqlGenerationHelper.StatementTerminator.Length);
            }
            return sql;
        }

        /// <summary>
        /// Generates a SQL statement for reading the current concurrency token value of a row. This is used
        /// for transactions that use mutations instead of DML, as mutations cannot include a WHERE clause.
        /// 
        /// The query will return a single row result set if the concurrency token value has the expected value,
        /// and an empty result set if the value has changed.
        /// </summary>
        internal string GenerateSelectConcurrencyCheckSql(
            string table,
            IReadOnlyList<IColumnModification> conditionOperations)
        {
            var concurrencyBuilder = new StringBuilder($"SELECT 1 FROM `{table}` ");
            AppendWhereClause(concurrencyBuilder, conditionOperations);
            return concurrencyBuilder.ToString();
        }

        public virtual ResultSetMapping AppendBulkInsertOperation(
            StringBuilder commandStringBuilder,
            IReadOnlyList<IReadOnlyModificationCommand> modificationCommands,
            int commandPosition)
        {
            if (modificationCommands.Count == 1
                && modificationCommands[0].ColumnModifications.All(
                    o =>
                        !o.IsKey
                        || !o.IsRead))
            {
                return AppendInsertOperation(commandStringBuilder, modificationCommands[0], commandPosition);
            }

            return AppendBulkInsertValues(commandStringBuilder, modificationCommands);
        }

        private ResultSetMapping AppendBulkInsertValues(
            StringBuilder commandStringBuilder,
            IReadOnlyList<IReadOnlyModificationCommand> modificationCommands)
        {
            var writeOperations = modificationCommands[0].ColumnModifications.ToList();

            Debug.Assert(writeOperations.Count > 0);

            var name = modificationCommands[0].TableName;
            var schema = modificationCommands[0].Schema;

            AppendInsertCommandHeader(commandStringBuilder, name, schema, writeOperations);
            AppendValuesHeader(commandStringBuilder, writeOperations);
            AppendValues(commandStringBuilder, name, schema, writeOperations);
            for (var i = 1; i < modificationCommands.Count; i++)
            {
                commandStringBuilder.AppendLine(",");
                AppendValues(commandStringBuilder, name, schema, modificationCommands[i].ColumnModifications.Where(o => o.IsWrite).ToList());
            }

            commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);

            return ResultSetMapping.NoResults;
        }
    }
}
