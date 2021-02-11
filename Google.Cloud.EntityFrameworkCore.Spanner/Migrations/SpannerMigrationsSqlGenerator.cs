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
using Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
            {"BYTES", "BYTES(MAX)"},
            {"ARRAY<STRING>", "ARRAY<STRING(MAX)>"},
            {"ARRAY<BYTES>", "ARRAY<BYTES(MAX)>"}
        };

        public SpannerMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
            : base(dependencies)
        {
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

            var nullFilteredIndexAnnotation = operation.FindAnnotation(SpannerAnnotationNames.IsNullFilteredIndex);
            if (nullFilteredIndexAnnotation != null && (bool)nullFilteredIndexAnnotation.Value)
            {
                builder.Append("NULL_FILTERED ");
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
            DropIndexOperation operation,
            IModel model,
            MigrationCommandListBuilder builder,
            bool terminate)
        {
            GaxPreconditions.CheckNotNull(operation, nameof(operation));
            GaxPreconditions.CheckNotNull(builder, nameof(builder));

            builder
                .Append(" DROP INDEX ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));

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
                GenerateCreateDatabase(createDatabaseOperation.Name, builder);
            }
            else if (operation is SpannerDropDatabaseOperation dropDatabaseOperation)
            {
                GenerateDropDatabase(dropDatabaseOperation.Name, builder);
            }
            else
            {
                base.Generate(operation, model, builder);
            }
        }

        private void GenerateDropDatabase(string name, MigrationCommandListBuilder builder)
        {
            builder
                .Append("DROP DATABASE ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name));

            EndStatement(builder, true);
        }

        private void GenerateCreateDatabase(string name, MigrationCommandListBuilder builder)
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
                // Exclude Interleaved table from foreign key.
                var isInterleavedWithParent = operation.FindAnnotation(SpannerAnnotationNames.InterleaveInParent) != null;
                builder.AppendLine(",");
                foreach (var key in operation.ForeignKeys)
                {
                    if (isInterleavedWithParent)
                    {
                        if (!key.Columns.Any(c => operation.PrimaryKey.Columns.Any(i => i == c)))
                        {
                            ForeignKeyConstraint(key, model, builder);
                            builder.AppendLine(",");
                        }
                    }
                    else
                    {
                        ForeignKeyConstraint(key, model, builder);
                        builder.AppendLine(",");
                    }
                }
            }
            else
            {
                builder.AppendLine();
            }

            if (operation.CheckConstraints.Any())
            {
                foreach (var checkConstraint in operation.CheckConstraints)
                {
                    CheckConstraint(checkConstraint, model, builder);
                    builder.AppendLine(",");
                }
            }

            builder.Append(")");
            if (operation.PrimaryKey != null)
            {
                PrimaryKeyConstraint(operation.PrimaryKey, model, builder);
            }

            var tableAttribute = operation.FindAnnotation(SpannerAnnotationNames.InterleaveInParent);
            if (tableAttribute != null)
            {
                var parentEntity = tableAttribute.Value.ToString();
                var parentTableName = model.FindEntityType(parentEntity).GetTableName();
                builder.AppendLine(",")
                    .Append(" INTERLEAVE IN PARENT ")
                    .Append(parentTableName)
                    .Append(" ON DELETE ");
                var onDeleteAtrribute = operation.FindAnnotation(SpannerAnnotationNames.InterleaveInParentOnDelete);
                if ((OnDelete)onDeleteAtrribute?.Value == OnDelete.Cascade)
                {
                    builder.AppendLine("CASCADE ");
                }
                else
                {
                    builder.AppendLine("NO ACTION ");
                }
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
            builder.Append(" CONSTRAINT ")
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

            if (operation.ComputedColumnSql != null)
            {
                ComputedColumnDefinition(schema, table, name, operation, model, builder);
                return;
            }

            var columnType = operation.ColumnType ?? GetColumnType(schema, table, name, operation, model);
            builder
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name))
                .Append(" ")
                .Append(GetCorrectedColumnType(columnType));

            if (!operation.IsNullable)
            {
                builder.Append(" NOT NULL");
            }

            var commitTimestampAnnotation = operation.FindAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp);
            if (commitTimestampAnnotation != null)
            {
                builder.Append(" OPTIONS (allow_commit_timestamp=true) ");
            }
        }

        protected override void ComputedColumnDefinition(
            string schema,
            string table,
            string name,
            ColumnOperation operation,
            IModel model,
            MigrationCommandListBuilder builder)
        {
            builder
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name))
                .Append(" ")
                .Append(GetCorrectedColumnType(operation.ColumnType ?? GetColumnType(schema, table, name, operation, model)));

            if (!operation.IsNullable)
            {
                builder.Append(" NOT NULL");
            }

            builder
                .Append(" AS ")
                .Append(operation.ComputedColumnSql);
        }

        protected override void Generate(
            InsertDataOperation operation,
            IModel model,
            MigrationCommandListBuilder builder,
            bool terminate = true)
        {

            var sqlBuilder = new StringBuilder();
            ((SpannerUpdateSqlGenerator)Dependencies.UpdateSqlGenerator).AppendBulkInsertOperation(
                sqlBuilder,
                operation.GenerateModificationCommands(model).ToList(),
                0);

            builder.Append(sqlBuilder.ToString());

            if (terminate)
            {
                builder.EndCommand();
            }
        }

        protected override void Generate(CreateSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner does not support sequence generation feature.");
        }

        protected override void Generate(AlterSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner does not support sequence generation feature.");
        }

        protected override void Generate(RenameSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner does not support sequence generation feature.");
        }

        protected override void Generate(RestartSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner does not support sequence generation feature.");
        }

        protected override void Generate(DropSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner does not support sequence generation feature.");
        }

        protected override void Generate(RenameColumnOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner does not support renaming columns.");
        }

        protected override void Generate(AlterDatabaseOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner Entity Framework Provider does not support AlterDatabaseOperation.");
        }

        protected override void Generate(RenameIndexOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner does not support renaming indexes.");
        }

        protected override void Generate(RenameTableOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner does not support renaming tables.");
        }

        protected override void Generate(EnsureSchemaOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner does not support creating schema.");
        }

        protected override void Generate(DropSchemaOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException($"Cloud Spanner does not support dropping schema.");
        }
    }
}
