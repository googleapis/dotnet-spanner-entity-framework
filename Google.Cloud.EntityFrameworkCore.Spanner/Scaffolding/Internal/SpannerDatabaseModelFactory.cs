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

using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Scaffolding.Internal
{
    public class SpannerDatabaseModelFactory : DatabaseModelFactory
    {
        public override DatabaseModel Create(string connectionString, DatabaseModelFactoryOptions options)
        {
            using var connection = new SpannerConnection(connectionString);
            return Create(connection, options);
        }

        public override DatabaseModel Create(DbConnection connection, DatabaseModelFactoryOptions options)
        {
            var databaseModel = new DatabaseModel();

            var connectionStartedOpen = connection.State == ConnectionState.Open;
            if (!connectionStartedOpen)
            {
                connection.Open();
            }

            try
            {
                databaseModel.DatabaseName = connection.Database;

                foreach (var table in GetTables(connection))
                {
                    table.Database = databaseModel;
                    databaseModel.Tables.Add(table);
                }

                return databaseModel;
            }
            finally
            {
                if (!connectionStartedOpen)
                {
                    connection.Close();
                }
            }
        }

        private IEnumerable<DatabaseTable> GetTables(DbConnection connection)
        {
            using var command = connection.CreateCommand();
            var tables = new List<DatabaseTable>();

            var commandText = @"SELECT  table_name FROM  information_schema.tables WHERE  table_catalog = '' and table_schema = ''";

            command.CommandText = commandText;

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var name = reader.GetValueOrDefault<string>("table_name");

                    var table = new DatabaseTable()
                    {
                        Name = name
                    };

                    tables.Add(table);
                }
            }

            GetColumns(connection, tables);
            GetColumnOptions(connection, tables);
            GetIndexes(connection, tables);
            GetForeignKeys(connection, tables);
            GetCheckConstraints(connection, tables);

            return tables;
        }

        private void GetColumns(
            DbConnection connection,
            IReadOnlyList<DatabaseTable> tables)
        {
            using var command = connection.CreateCommand();
            var commandText = @"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE  TABLE_CATALOG = '' AND TABLE_SCHEMA = '' ORDER BY TABLE_NAME, ORDINAL_POSITION";

            command.CommandText = commandText;

            using var reader = command.ExecuteReader();
            var tableColumnGroups = reader.Cast<DbDataRecord>()
                .GroupBy(ddr => ddr.GetValueOrDefault<string>("TABLE_NAME"));

            foreach (var tableColumnGroup in tableColumnGroups)
            {
                var tableName = tableColumnGroup.Key;

                var table = tables.Single(t => t.Name == tableName);

                foreach (var dataRecord in tableColumnGroup)
                {
                    var columnName = dataRecord.GetValueOrDefault<string>("COLUMN_NAME");
                    var dataTypeName = dataRecord.GetValueOrDefault<string>("SPANNER_TYPE");
                    dataTypeName = dataTypeName.Replace("STRING(MAX)", $"STRING({SpannerTypeMappingSource.StringMax})");
                    dataTypeName = dataTypeName.Replace("BYTES(MAX)", $"BYTES({SpannerTypeMappingSource.BytesMax})");
                    var nullable = dataRecord.GetValueOrDefault<string>("IS_NULLABLE") == "YES";
                    var defaultValue = dataRecord.GetValueOrDefault<string>("COLUMN_DEFAULT");
                    var generated = dataRecord.GetValueOrDefault<string>("IS_GENERATED") != "NEVER";
                    var generationExpression = dataRecord.GetValueOrDefault<string>("GENERATION_EXPRESSION");
                    var valueGenerated = generated ? ValueGenerated.OnAddOrUpdate : (ValueGenerated?)null;

                    var column = new DatabaseColumn
                    {
                        Table = table,
                        Name = columnName,
                        StoreType = dataTypeName,
                        IsNullable = nullable,
                        DefaultValueSql = defaultValue,
                        // TODO: The combination of ComputedColumnSql and ValueGenerated leads to the computed SQL
                        //       taking precedence, and the ValueGenerated property not to be generated. That again
                        //       causes INSERT statements to be generated with the computed column in the columns list.
                        // ComputedColumnSql = generationExpression,
                        ValueGenerated = valueGenerated,
                    };
                    table.Columns.Add(column);
                }
            }
        }

        private void GetColumnOptions(
            DbConnection connection,
            IReadOnlyList<DatabaseTable> tables)
        {
            using var command = connection.CreateCommand();
            var commandText = @"SELECT * 
                                    FROM INFORMATION_SCHEMA.COLUMN_OPTIONS
                                    WHERE TABLE_CATALOG = '' AND TABLE_SCHEMA = ''
                                    ORDER BY TABLE_NAME, COLUMN_NAME";

            command.CommandText = commandText;

            using var reader = command.ExecuteReader();
            var tableColumnGroups = reader.Cast<DbDataRecord>()
                .GroupBy(ddr => ddr.GetValueOrDefault<string>("TABLE_NAME"));

            foreach (var tableColumnGroup in tableColumnGroups)
            {
                var tableName = tableColumnGroup.Key;

                var table = tables.Single(t => t.Name == tableName);

                foreach (var dataRecord in tableColumnGroup)
                {
                    var columnName = dataRecord.GetValueOrDefault<string>("COLUMN_NAME");
                    var optionName = dataRecord.GetValueOrDefault<string>("OPTION_NAME");
                    var optionValue = dataRecord.GetValueOrDefault<string>("OPTION_VALUE");

                    var column = table.Columns.Single(c => c.Name == columnName);

                    if (optionName == "allow_commit_timestamp" && bool.TryParse(optionValue, out _))
                    {
                        column.AddAnnotation(SpannerAnnotationNames.UpdateCommitTimestamp, SpannerUpdateCommitTimestamp.OnInsertAndUpdate);
                    }
                }
            }
        }

        private void GetIndexes(DbConnection connection, IReadOnlyList<DatabaseTable> tables)
        {
            using var command = connection.CreateCommand();
            var commandText = @"SELECT  * FROM  INFORMATION_SCHEMA.INDEX_COLUMNS WHERE table_catalog = '' and table_schema = ''  order by table_name,INDEX_NAME";

            command.CommandText = commandText;

            using var reader = command.ExecuteReader();
            var tableIndexGroups = reader.Cast<DbDataRecord>()
                .GroupBy(ddr => ddr.GetValueOrDefault<string>("TABLE_NAME"));

            foreach (var tableIndexGroup in tableIndexGroups)
            {
                var tableName = tableIndexGroup.Key;

                var table = tables.Single(t => t.Name == tableName);

                var primaryKeyGroups = tableIndexGroup
                    .Where(ddr => ddr.GetValueOrDefault<string>("INDEX_TYPE") == "PRIMARY_KEY")
                    .GroupBy(
                        ddr =>
                            (Name: ddr.GetValueOrDefault<string>("INDEX_NAME"),
                                TypeDesc: ddr.GetValueOrDefault<string>("INDEX_TYPE")))
                    .ToArray();

                if (primaryKeyGroups.Length == 1)
                {
                    var primaryKeyGroup = primaryKeyGroups[0];


                    var primaryKey = new DatabasePrimaryKey { Table = table, Name = primaryKeyGroup.Key.Name };

                    if (primaryKeyGroup.Key.TypeDesc == "INDEX")
                    {
                        primaryKey["Spanner:Clustered"] = false;
                    }

                    foreach (var dataRecord in primaryKeyGroup)
                    {
                        var columnName = dataRecord.GetValueOrDefault<string>("COLUMN_NAME");
                        var column = table.Columns.FirstOrDefault(c => c.Name == columnName)
                            ?? table.Columns.FirstOrDefault(
                                c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));

                        primaryKey.Columns.Add(column);
                    }

                    table.PrimaryKey = primaryKey;
                }

                var indexGroups = tableIndexGroup.Where(
                    ddr => ddr.GetValueOrDefault<string>("INDEX_TYPE") != "PRIMARY_KEY")
                    .GroupBy(ddr =>
                    (Name: ddr.GetValueOrDefault<string>("INDEX_NAME"),
                    TypeDesc: ddr.GetValueOrDefault<string>("INDEX_TYPE")))
                    .ToArray();

                foreach (var indexGroup in indexGroups)
                {
                    var index = new DatabaseIndex
                    {
                        Table = table,
                        Name = indexGroup.Key.Name,
                    };

                    if (indexGroup.Key.TypeDesc == "PRIMARY_KEY")
                    {
                        index["Spanner:Clustered"] = true;
                    }

                    foreach (var dataRecord in indexGroup)
                    {
                        var columnName = dataRecord.GetValueOrDefault<string>("column_name");
                        var column = table.Columns.FirstOrDefault(c => c.Name == columnName)
                            ?? table.Columns.FirstOrDefault(
                                c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                        index.Columns.Add(column);
                    }

                    table.Indexes.Add(index);
                }

            }
        }

        private void GetForeignKeys(DbConnection connection, IReadOnlyList<DatabaseTable> tables)
        {
            using var command = connection.CreateCommand();
            var commandText =
                "SELECT * FROM ("
                + "SELECT FK.TABLE_CATALOG AS FK_CATALOG, FK.TABLE_SCHEMA AS FK_SCHEMA, FK.TABLE_NAME AS FK_TABLE, FK.CONSTRAINT_NAME, FK_COLS.COLUMN_NAME AS FK_COL_NAME, PK_COLS.TABLE_NAME AS PK_TABLE, PK_COLS.COLUMN_NAME AS PK_COL_NAME, FK_COLS.POSITION_IN_UNIQUE_CONSTRAINT AS ORDINAL_POSITION "
                + "from INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK "
                + "inner join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC ON "
                + "    FK.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG AND "
                + "    FK.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA AND "
                + "    FK.CONSTRAINT_NAME = RC.CONSTRAINT_NAME AND "
                + "    FK.CONSTRAINT_TYPE = 'FOREIGN KEY' "
                + "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE FK_COLS ON "
                + "    FK.CONSTRAINT_CATALOG = FK_COLS.CONSTRAINT_CATALOG AND "
                + "    FK.CONSTRAINT_SCHEMA = FK_COLS.CONSTRAINT_SCHEMA AND "
                + "    FK.CONSTRAINT_NAME = FK_COLS.CONSTRAINT_NAME "
                + "inner join INFORMATION_SCHEMA.KEY_COLUMN_USAGE PK_COLS ON "
                + "    RC.UNIQUE_CONSTRAINT_CATALOG = PK_COLS.CONSTRAINT_CATALOG AND "
                + "    RC.UNIQUE_CONSTRAINT_SCHEMA = PK_COLS.CONSTRAINT_SCHEMA AND "
                + "    RC.UNIQUE_CONSTRAINT_NAME = PK_COLS.CONSTRAINT_NAME AND "
                + "    FK_COLS.POSITION_IN_UNIQUE_CONSTRAINT = PK_COLS.ORDINAL_POSITION "
                + "where FK.CONSTRAINT_CATALOG = '' AND FK.CONSTRAINT_SCHEMA = '' "

                + "UNION ALL "

                + "SELECT CHILD.TABLE_CATALOG AS FK_CATALOG, CHILD.TABLE_SCHEMA AS FK_SCHEMA, CHILD.TABLE_NAME AS FK_TABLE, PK_PARENT.CONSTRAINT_NAME AS CONSTRAINT_NAME, FK_COLS.COLUMN_NAME AS FK_COL_NAME, PARENT.TABLE_NAME AS PK_TABLE, FK_COLS.COLUMN_NAME AS PK_COL_NAME, FK_COLS.ORDINAL_POSITION "
                + "FROM INFORMATION_SCHEMA.TABLES CHILD "
                + "INNER JOIN INFORMATION_SCHEMA.TABLES PARENT ON "
                + "    CHILD.TABLE_CATALOG = PARENT.TABLE_CATALOG AND "
                + "    CHILD.TABLE_SCHEMA = PARENT.TABLE_SCHEMA AND "
                + "    CHILD.PARENT_TABLE_NAME = PARENT.TABLE_NAME "
                + "INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK_CHILD ON "
                + "    CHILD.TABLE_CATALOG = PK_CHILD.TABLE_CATALOG AND "
                + "    CHILD.TABLE_SCHEMA = PK_CHILD.TABLE_SCHEMA AND "
                + "    CHILD.TABLE_NAME = PK_CHILD.TABLE_NAME AND "
                + "    PK_CHILD.CONSTRAINT_TYPE = 'PRIMARY KEY' "
                + "INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK_PARENT ON "
                + "    PARENT.TABLE_CATALOG = PK_PARENT.TABLE_CATALOG AND "
                + "    PARENT.TABLE_SCHEMA = PK_PARENT.TABLE_SCHEMA AND "
                + "    PARENT.TABLE_NAME = PK_PARENT.TABLE_NAME AND "
                + "    PK_PARENT.CONSTRAINT_TYPE = 'PRIMARY KEY' "
                + "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE FK_COLS ON "
                + "    PK_PARENT.CONSTRAINT_CATALOG = FK_COLS.CONSTRAINT_CATALOG AND "
                + "    PK_PARENT.CONSTRAINT_SCHEMA = FK_COLS.CONSTRAINT_SCHEMA AND "
                + "    PK_PARENT.CONSTRAINT_NAME = FK_COLS.CONSTRAINT_NAME "
                + "WHERE CHILD.TABLE_CATALOG = '' AND CHILD.TABLE_SCHEMA = '' "

                + ") FK "
                + "ORDER BY FK_CATALOG, FK_SCHEMA, FK_TABLE, CONSTRAINT_NAME, ORDINAL_POSITION ";

            command.CommandText = commandText;

            using var reader = command.ExecuteReader();
            var tableFkGroups = reader.Cast<DbDataRecord>()
                .GroupBy(ddr => (
                    FkTable: ddr.GetValueOrDefault<string>("FK_TABLE"),
                    PkTable: ddr.GetValueOrDefault<string>("PK_TABLE"),
                    Name: ddr.GetValueOrDefault<string>("CONSTRAINT_NAME")
                ));

            foreach (var tableFkGroup in tableFkGroups)
            {
                var fkTableName = tableFkGroup.Key.FkTable;
                var fkTable = tables.Single(t => t.Name == fkTableName);
                var pkTableName = tableFkGroup.Key.PkTable;
                var pkTable = tables.Single(t => t.Name == pkTableName);
                var fk = new DatabaseForeignKey { Name = tableFkGroup.Key.Name, Table = fkTable, PrincipalTable = pkTable, OnDelete = ReferentialAction.Restrict };

                foreach (var col in tableFkGroup)
                {
                    var fkCol = fkTable.Columns.FirstOrDefault(c => c.Name == col.GetValueOrDefault<string>("FK_COL_NAME"));
                    fk.Columns.Add(fkCol);
                    var pkCol = pkTable.Columns.FirstOrDefault(c => c.Name == col.GetValueOrDefault<string>("PK_COL_NAME"));
                    fk.PrincipalColumns.Add(pkCol);
                }

                fkTable.ForeignKeys.Add(fk);
            }
        }

        private void GetCheckConstraints(DbConnection connection, IReadOnlyList<DatabaseTable> tables)
        {
            using var command = connection.CreateCommand();
            // Unfortunately the INFORMATION_SCHEMA tables of Cloud Spanner does not allow us to exclude
            // NOT NULL constraints from the search without doing so using the generated constraint name.
            var commandText = @"
                SELECT TBL.TABLE_NAME, CHK.CONSTRAINT_NAME, CHK.CHECK_CLAUSE 
                FROM       INFORMATION_SCHEMA.CHECK_CONSTRAINTS CHK
                INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE TBL
                        ON CHK.CONSTRAINT_CATALOG = TBL.CONSTRAINT_CATALOG
                       AND CHK.CONSTRAINT_SCHEMA  = TBL.CONSTRAINT_SCHEMA
                       AND CHK.CONSTRAINT_NAME    = TBL.CONSTRAINT_NAME
                WHERE CHK.CONSTRAINT_CATALOG = '' AND CHK.CONSTRAINT_SCHEMA = '' AND NOT STARTS_WITH(CHK.CONSTRAINT_NAME, 'CK_IS_NOT_NULL_')
                ORDER BY TBL.TABLE_CATALOG, TBL.TABLE_SCHEMA, TBL.TABLE_NAME, CHK.CONSTRAINT_NAME";

            command.CommandText = commandText;

            using var reader = command.ExecuteReader();
            var tableGroups = reader.Cast<DbDataRecord>()
                .GroupBy(ddr => ddr.GetValueOrDefault<string>("TABLE_NAME"));

            foreach (var tableGroup in tableGroups)
            {
                var table = tables.Single(t => t.Name == tableGroup.Key);
                foreach (var constraint in tableGroup)
                {
                    var constraintName = constraint.GetValueOrDefault<string>("CONSTRAINT_NAME");
                    var checkClause = constraint.GetValueOrDefault<string>("CHECK_CLAUSE");
                    // EF Core 3 does not support adding Check Constraints to the meta model.
                    // Adding it as an annotation at the moment.
                    table.AddAnnotation($"CONSTRAINT `{constraintName}`", $"CHECK {checkClause}");
                }
            }
        }
    }
}
