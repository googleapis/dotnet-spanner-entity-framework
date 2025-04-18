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
using Google.Cloud.EntityFrameworkCore.Spanner.Metadata;
using Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Operations;
using Google.Cloud.EntityFrameworkCore.Spanner.Update.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.EntityFrameworkCore.Update;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Migrations
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
            AlterColumnOperation operation,
            IModel model,
            MigrationCommandListBuilder builder)
        {
            builder
               .Append("ALTER TABLE ")
               .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
               .Append(" ALTER COLUMN ");

            var commitTimestampAnnotation = operation.FindAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp);
            if (commitTimestampAnnotation != null)
            {
                if ((SpannerUpdateCommitTimestamp)commitTimestampAnnotation.Value != SpannerUpdateCommitTimestamp.Never)
                {
                    builder
                        .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
                        .Append(" SET OPTIONS (allow_commit_timestamp=true) ");
                }
                else
                {
                    builder
                        .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
                        .Append(" SET OPTIONS (allow_commit_timestamp=null) ");
                }
            }
            else
            {
                var definitionOperation = new AlterColumnOperation
                {
                    Schema = operation.Schema,
                    Table = operation.Table,
                    Name = operation.Name,
                    ClrType = operation.ClrType,
                    ColumnType = operation.ColumnType,
                    IsUnicode = operation.IsUnicode,
                    IsFixedLength = operation.IsFixedLength,
                    MaxLength = operation.MaxLength,
                    IsRowVersion = operation.IsRowVersion,
                    IsNullable = operation.IsNullable,
                    ComputedColumnSql = operation.ComputedColumnSql,
                    OldColumn = operation.OldColumn
                };

                ColumnDefinition(
                    operation.Schema,
                    operation.Table,
                    operation.Name,
                    definitionOperation,
                    model,
                    builder);

                if (operation.DefaultValue != null
                   || operation.DefaultValueSql != null)
                {
                    DefaultValue(operation.DefaultValue, operation.DefaultValueSql, operation.ColumnType, builder);
                }
            }
            builder.EndCommand();
        }

        protected override void Generate(
            CreateIndexOperation operation,
            IModel model,
            MigrationCommandListBuilder builder,
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
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
                .Append(" ON ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
                .Append(" (")
                .Append(ColumnList(operation.Columns))
                .Append(")");

            if (operation[SpannerAnnotationNames.Storing] is string[] storingColumns
                            && storingColumns.Length > 0)
            {
                builder.Append(" STORING (")
                    .Append(ColumnList(storingColumns))
                    .Append(")");
            }

            if (terminate)
            {
                EndStatement(builder, true);
            }
        }

        protected override void Generate(
            DropIndexOperation operation,
            IModel model,
            MigrationCommandListBuilder builder,
            bool terminate = true)
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

        protected override void Generate(
            MigrationOperation operation,
            IModel model,
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
            bool terminate = true)
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
                    .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(parentTableName))
                    .Append(" ON DELETE ");
                var onDeleteAttribute = operation.FindAnnotation(SpannerAnnotationNames.InterleaveInParentOnDelete);
                builder.AppendLine(onDeleteAttribute != null && (OnDelete)onDeleteAttribute.Value == OnDelete.Cascade ? "CASCADE " : "NO ACTION ");
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
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
                .Append(" FOREIGN KEY (")
                .Append(ColumnList(operation.Columns))
                .Append(") REFERENCES ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.PrincipalTable, operation.PrincipalSchema))
                .Append(" (")
                .Append(ColumnList(operation.PrincipalColumns))
                .Append(")");
        }

        private static string GetCorrectedColumnType(string columnType, int? maxLength)
        {
            columnType = columnType.ToUpperInvariant();
            return s_columnTypeMap.TryGetValue(columnType, out string convertedColumnType)
                ? (maxLength ?? 0) > 0 ? convertedColumnType.Replace("(MAX)", $"({maxLength})") : convertedColumnType
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
                .Append(GetCorrectedColumnType(columnType, operation.MaxLength));

            if (!operation.IsNullable)
            {
                builder.Append(" NOT NULL");
            }
            var identityAnnotation = operation.FindAnnotation(SpannerAnnotationNames.Identity);
            if (operation.DefaultValue != null || operation.DefaultValueSql != null)
            {
                DefaultValue(operation.DefaultValue, operation.DefaultValueSql, columnType, builder);
            }
            else if (identityAnnotation is { Value: SpannerIdentityOptionsData value })
            {
                if (value.GenerationStrategy == GenerationStrategy.GeneratedByDefault)
                {
                    builder.Append(" GENERATED BY DEFAULT AS IDENTITY");
                }
                else if (value.GenerationStrategy == GenerationStrategy.GeneratedAlways)
                {
                    builder.Append(" GENERATED ALWAYS AS IDENTITY");
                }
                else if (value.GenerationStrategy == GenerationStrategy.AutoIncrement)
                {
                    builder.Append(" AUTO_INCREMENT");
                }
                if (value.SequenceKind != null && value.GenerationStrategy is GenerationStrategy.GeneratedByDefault or GenerationStrategy.GeneratedAlways)
                {
                    builder.Append(" (").Append(value.SequenceKind).Append(")");
                }
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
                .Append(GetCorrectedColumnType(operation.ColumnType ?? GetColumnType(schema, table, name, operation, model), operation.MaxLength));

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
            var commands = GenerateModificationCommands(operation, model);
            var sqlBuilder = new StringBuilder();
            ((SpannerUpdateSqlGenerator)Dependencies.UpdateSqlGenerator).AppendBulkInsertOperation(
                sqlBuilder,
                commands.ToList(),
                0);

            builder.Append(sqlBuilder.ToString());

            if (terminate)
            {
                builder.EndCommand();
            }
        }

        protected override void Generate(CreateSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            GaxPreconditions.CheckNotNull(operation, nameof(operation));
            GaxPreconditions.CheckNotNull(builder, nameof(builder));
            builder.Append("CREATE SEQUENCE ").Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema));
            SequenceOptions(operation, model, builder);
            EndStatement(builder);
        }

        protected override void SequenceOptions(
            string schema,
            string name,
            SequenceOperation operation,
            IModel model,
            MigrationCommandListBuilder builder)
        {
            builder.Append(" OPTIONS (sequence_kind=bit_reversed_positive");
            if (operation is CreateSequenceOperation createSequenceOperation && createSequenceOperation.StartValue != 1)
            {
                RelationalTypeMapping mapping = Dependencies.TypeMappingSource.GetMapping(createSequenceOperation.ClrType);
                builder.Append(", start_with_counter=").Append(mapping.GenerateSqlLiteral(createSequenceOperation.StartValue));
            }
            builder.Append(")").AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        }

        protected override void Generate(AlterSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException("Spanner Entity Framework does not support alter sequence operations.");
        }

        protected override void Generate(RenameSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException("Spanner does not support renaming sequences.");
        }

        protected override void Generate(RestartSequenceOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            GaxPreconditions.CheckNotNull(operation, nameof(operation));
            GaxPreconditions.CheckNotNull(builder, nameof(builder));
            RelationalTypeMapping mapping = Dependencies.TypeMappingSource.GetMapping(typeof (long));
            builder.Append("ALTER SEQUENCE ").Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name, operation.Schema))
                .Append(" OPTIONS (start_with_counter=")
                .Append(mapping.GenerateSqlLiteral(operation.StartValue))
                .Append(")")
                .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            EndStatement(builder);
        }

        protected override void Generate(RenameColumnOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException("Cloud Spanner does not support renaming columns.");
        }

        protected override void Generate(AlterDatabaseOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException("Cloud Spanner Entity Framework Provider does not support AlterDatabaseOperation.");
        }

        protected override void Generate(RenameIndexOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException("Cloud Spanner does not support renaming indexes.");
        }

        protected override void Generate(RenameTableOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException("Cloud Spanner does not support renaming tables.");
        }

        protected override void Generate(EnsureSchemaOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException("Cloud Spanner does not support creating schema.");
        }

        protected override void Generate(DropSchemaOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException("Cloud Spanner does not support dropping schema.");
        }

        protected override void UniqueConstraint(AddUniqueConstraintOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException("Cloud Spanner does not support unique constraints. Use unique indexes instead.");
        }

        protected override void Generate(DropUniqueConstraintOperation operation, IModel model, MigrationCommandListBuilder builder)
        {
            throw new NotSupportedException("Cloud Spanner does not support unique constraints. Use unique indexes instead.");
        }

        protected override void Generate(AddPrimaryKeyOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate = true)
        {
            throw new NotSupportedException("Cloud Spanner does not support adding a primary key to an existing table. The primary key must always be included in the CREATE TABLE statement.");
        }

        protected override void Generate(DropPrimaryKeyOperation operation, IModel model, MigrationCommandListBuilder builder, bool terminate = true)
        {
            throw new NotSupportedException("Cloud Spanner does not support dropping a primary key. All tables must always have a primary key.");
        }

        protected override void DefaultValue(object defaultValue, string defaultValueSql, string columnType, MigrationCommandListBuilder builder)
        {
            if (defaultValueSql != null)
            {
                base.DefaultValue(defaultValue, defaultValueSql, columnType, builder);
            }
            else if (defaultValue != null)
            {
                var typeMapping = (columnType != null
                        ? Dependencies.TypeMappingSource.FindMapping(defaultValue.GetType(), columnType)
                        : null)
                    ?? Dependencies.TypeMappingSource.GetMappingForValue(defaultValue);

                builder
                    .Append(" DEFAULT (")
                    .Append(typeMapping.GenerateSqlLiteral(defaultValue))
                    .Append(")");
            }
        }
    }
}
