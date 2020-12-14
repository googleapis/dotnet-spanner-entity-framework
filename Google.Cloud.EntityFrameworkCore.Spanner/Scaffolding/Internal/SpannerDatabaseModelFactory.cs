using Google.Cloud.Spanner.Data;
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
            using (var connection = new SpannerConnection(connectionString))
            {
                return Create(connection, options);
            }
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
            using (var command = connection.CreateCommand())
            {
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
                GetIndexes(connection, tables);
                //GetForeignKeys(connection, tables);

                return tables;
            }
        }

        private void GetColumns(
            DbConnection connection,
            IReadOnlyList<DatabaseTable> tables)
        {
            using (var command = connection.CreateCommand())
            {
                var commandText = @"SELECT  * FROM  INFORMATION_SCHEMA.COLUMNS WHERE  table_catalog = '' and table_schema = ''  order by table_name,ORDINAL_POSITION";

                command.CommandText = commandText;

                using (var reader = command.ExecuteReader())
                {
                    var tableColumnGroups = reader.Cast<DbDataRecord>()
                        .GroupBy(ddr => ddr.GetValueOrDefault<string>("table_name"));

                    foreach (var tableColumnGroup in tableColumnGroups)
                    {
                        var tableName = tableColumnGroup.Key;

                        var table = tables.Single(t => t.Name == tableName);

                        foreach (var dataRecord in tableColumnGroup)
                        {
                            var columnName = dataRecord.GetValueOrDefault<string>("COLUMN_NAME");
                            var dataTypeName = dataRecord.GetValueOrDefault<string>("SPANNER_TYPE");
                            var nullable = dataRecord.GetValueOrDefault<string>("IS_NULLABLE") == "YES" ? true : false;
                            var defaultValue = dataRecord.GetValueOrDefault<string>("COLUMN_DEFAULT");

                            var column = new DatabaseColumn
                            {
                                Table = table,
                                Name = columnName,
                                StoreType = dataTypeName,
                                IsNullable = nullable,
                                DefaultValueSql = defaultValue,
                            };
                            table.Columns.Add(column);
                        }
                    }
                }
            }
        }

        private void GetIndexes(DbConnection connection, IReadOnlyList<DatabaseTable> tables)
        {
            using (var command = connection.CreateCommand())
            {
                var commandText = @"SELECT  * FROM  INFORMATION_SCHEMA.INDEX_COLUMNS WHERE table_catalog = '' and table_schema = ''  order by table_name,INDEX_NAME";

                command.CommandText = commandText;

                using (var reader = command.ExecuteReader())
                {
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
            }
        }
    }
}
