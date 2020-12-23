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
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.EntityFrameworkCore.Migrations
{
    /// <summary>
    /// Customizes the default migration sql generator to create Spanner compatible Ddl.
    /// </summary>
    internal class SpannerMigrationsSqlGenerator : MigrationsSqlGenerator
    {
        //When creating tables, we always need to specify "MAX"
        //However, its important that the simple typename remain "STRING" because throughout query operations,
        //the typename is used for CAST and other operations where "MAX" would result in an error.
        private static readonly Dictionary<string, string> s_columnTypeMap = new Dictionary<string, string>
        {
            {"STRING", "STRING(MAX)"},
            {"BYTES", "BYTES(MAX)"}
        };

        public SpannerMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
            : base(dependencies)
        {
        }

        protected override void Generate(
                    AddColumnOperation operation,
                    IModel model,
                    MigrationCommandListBuilder builder,
                    bool terminate)
        {
            base.Generate(operation, model, builder, terminate: false);

            if (terminate)
            {
                builder.EndCommand();
            }
        }

        protected override void Generate(
            [NotNull] CreateIndexOperation operation,
            [CanBeNull] IModel model,
            [NotNull] MigrationCommandListBuilder builder,
            bool terminate = true)
        {
            builder.Append("CREATE ");
            if (operation.IsUnique)
            {
                builder.Append("UNIQUE ");
            }
            builder.Append("INDEX ")
                .Append(operation.Name)
                .Append(" ON ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
                .Append(" (")
                .Append(ColumnList(operation.Columns))
                .Append(")");

            if (terminate)
            {
                EndStatement(builder, true);
            }
        }

        protected override void Generate(
            [NotNull] DropTableOperation operation,
            [CanBeNull] IModel model,
            [NotNull] MigrationCommandListBuilder builder,
            bool terminate = true)
        {
            base.Generate(operation, model, builder, terminate: false);

            if (terminate)
            {
                builder.EndCommand();
            }
        }

        protected override void Generate(MigrationOperation operation, IModel model,
            MigrationCommandListBuilder builder)
        {
            if (operation is SpannerCreateDatabaseOperation createDatabaseOperation)
            {
                GenerateCreateDatabase(createDatabaseOperation.Name, model, builder);
            }
            else if (operation is SpannerDropDatabaseOperation dropDatabaseOperation)
            {
                GenerateDropDatabase(dropDatabaseOperation.Name, model, builder);
            }
            else
            {
                base.Generate(operation, model, builder);
            }
        }

        private void GenerateDropDatabase(string name, IModel model, MigrationCommandListBuilder builder)
        {
            builder
                .Append("DROP DATABASE ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name));

            EndStatement(builder, true);
        }

        private void GenerateCreateDatabase(string name, IModel model, MigrationCommandListBuilder builder)
        {
            builder
                .Append("CREATE DATABASE ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name));

            EndStatement(builder, true);
        }

        protected override void Generate(
            CreateTableOperation operation,
            IModel model,
            MigrationCommandListBuilder builder,
            bool terminate)
        {
            builder
                .Append("CREATE TABLE ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema))
                .AppendLine(" (");

            using (builder.Indent())
            {
                for (var i = 0; i < operation.Columns.Count; i++)
                {
                    var column = operation.Columns[i];
                    ColumnDefinition(column, model, builder);

                    if (i != operation.Columns.Count - 1)
                    {
                        builder.AppendLine(",");
                    }
                }
            }

            if (operation.ForeignKeys.Any())
            {
                builder.Append(",");
                builder.AppendLine();
                operation.ForeignKeys.ForEach(key =>
                {
                    ForeignKeyConstraint(key, model, builder);
                });
            }
            else
            {
                builder.AppendLine();
            }

            builder.Append(")");
            if (operation.PrimaryKey != null)
            {
                PrimaryKeyConstraint(operation.PrimaryKey, model, builder);
            }

            if (terminate)
            {
                EndStatement(builder, true);
            }
        }

        protected override void PrimaryKeyConstraint(
            AddPrimaryKeyOperation operation,
            IModel model,
            MigrationCommandListBuilder builder)
        {
            builder.Append("PRIMARY KEY ");
            IndexTraits(operation, model, builder);
            builder.Append("(")
                .Append(ColumnList(operation.Columns))
                .Append(")");
        }

        protected override void ForeignKeyConstraint(
            AddForeignKeyOperation operation,
            IModel model,
            MigrationCommandListBuilder builder)
        {
            builder.Append("CONSTRAINT ")
                .Append(operation.Name)
                .Append(" FOREIGN KEY (")
                .Append(ColumnList(operation.Columns))
                .Append(") REFERENCES ")
                .Append(operation.PrincipalTable)
                .Append(" (")
                .Append(ColumnList(operation.PrincipalColumns))
                .Append(")");
        }

        private static string GetCorrectedColumnType(string columnType)
        {
            columnType = columnType.ToUpperInvariant();
            return s_columnTypeMap.TryGetValue(columnType, out string convertedColumnType)
                ? convertedColumnType
                : columnType;
        }

        protected override void ColumnDefinition(
            string schema,
            string table,
            string name,
            ColumnOperation operation,
            IModel model,
            MigrationCommandListBuilder builder)
        {
            GaxPreconditions.CheckNotNullOrEmpty(name, nameof(name));
            GaxPreconditions.CheckNotNull(operation, nameof(operation));
            GaxPreconditions.CheckNotNull(builder, nameof(builder));

            builder
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name))
                .Append(" ")
                .Append(GetCorrectedColumnType(GetColumnType(schema, table, name, operation, model)));

            if (!operation.IsNullable)
            {
                builder.Append(" NOT NULL");
            }
        }
    }
}
