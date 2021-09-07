// Copyright 2021, Google Inc. All rights reserved.
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

using Google.Cloud.EntityFrameworkCore.Spanner.Extensions.Internal;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure.Internal
{
    internal class SpannerModelValidator : RelationalModelValidator
    {
        private readonly string _disableValidationHint = $"Call {nameof(SpannerModelValidationConnectionProvider)}.{nameof(SpannerModelValidationConnectionProvider.Instance)}.{nameof(SpannerModelValidationConnectionProvider.EnableDatabaseModelValidation)}(false) to disable model validation if this error is a false positive.";

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public SpannerModelValidator(
            SpannerModelValidationConnectionProvider connectionStringProvider,
            ModelValidatorDependencies dependencies,
            RelationalModelValidatorDependencies relationalDependencies)
            : base(dependencies, relationalDependencies)
            => ConnectionStringProvider = connectionStringProvider;

        internal SpannerModelValidationConnectionProvider ConnectionStringProvider { get; }

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public override void Validate(IModel model, IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
        {
            base.Validate(model, logger);
            ValidateKeysCorrespondWithDataModel(model);
        }

        /// <summary>
        /// Validates that all entities have a key definition and that the key definition corresponds with an actual
        /// non-null-filtered unique key in the database.
        /// </summary>
        protected virtual void ValidateKeysCorrespondWithDataModel(IModel model)
        {
            using var connection = ConnectionStringProvider.GetConnection();
            if (connection == null)
            {
                return;
            }
            // Check whether the model is being validated as part of a migration.
            // In that case we should also skip the further validation, as the differences
            // could be a result of migrations that still need to be executed.
            var facadeExtensions = nameof(RelationalDatabaseFacadeExtensions);
            var migrateMethods = new List<string>
            {
                nameof(RelationalDatabaseFacadeExtensions.Migrate),
                nameof(RelationalDatabaseFacadeExtensions.MigrateAsync),
            };
            var migrationsOperations = nameof(MigrationsOperations);
            var migrationsOperationsMethods = new List<string>
            {
                nameof(MigrationsOperations.AddMigration),
                nameof(MigrationsOperations.GetMigrations),
                nameof(MigrationsOperations.RemoveMigration),
                nameof(MigrationsOperations.ScriptMigration),
                nameof(MigrationsOperations.UpdateDatabase),
            };
            int skipFrames = 1;
            MethodBase method = null;
            do
            {
                method = new StackFrame(skipFrames, false).GetMethod();
                if (method != null && migrateMethods.Contains(method.Name) && facadeExtensions.Equals(method.DeclaringType.Name))
                {
                    return;
                }
                if (method != null && migrationsOperationsMethods.Contains(method.Name) && migrationsOperations.Equals(method.DeclaringType.Name))
                {
                    return;
                }
                skipFrames++;
            } while (method != null);

            try
            {
                var commandText = @"SELECT I.TABLE_NAME, I.INDEX_NAME, C.COLUMN_NAME 
                                    FROM INFORMATION_SCHEMA.INDEXES I
                                    INNER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS C ON
		                                    I.TABLE_CATALOG = C.TABLE_CATALOG AND
		                                    I.TABLE_SCHEMA = C.TABLE_SCHEMA AND
		                                    I.TABLE_NAME = C.TABLE_NAME AND
		                                    I.INDEX_NAME = C.INDEX_NAME
                                    WHERE I.TABLE_CATALOG = ''
                                      AND I.TABLE_SCHEMA = ''
                                      AND I.IS_UNIQUE = TRUE
                                      AND I.IS_NULL_FILTERED = FALSE
                                    ORDER BY I.TABLE_CATALOG, I.TABLE_SCHEMA, I.TABLE_NAME, I.INDEX_NAME, C.ORDINAL_POSITION";
                using var cmd = connection.CreateSelectCommand(commandText);
                using var reader = cmd.ExecuteReader();
                var tableKeyColumnsGroups = reader.Cast<DbDataRecord>()
                    .GroupBy(ddr => (
                        Table: ddr.GetString(ddr.GetOrdinal("TABLE_NAME")),
                        Index: ddr.GetString(ddr.GetOrdinal("INDEX_NAME"))));
                var tableKeyColumns = new Dictionary<string, List<List<string>>>();
                foreach (var tableKeyColumnGroup in tableKeyColumnsGroups)
                {
                    var table = tableKeyColumnGroup.Key.Table;
                    var index = tableKeyColumnGroup.Key.Index;
                    var columns = tableKeyColumnGroup.Select(col => col.GetString(col.GetOrdinal("COLUMN_NAME"))).ToList();
                    if (!tableKeyColumns.TryGetValue(table, out List<List<string>> allIndexedColumns))
                    {
                        allIndexedColumns = new List<List<string>>();
                        tableKeyColumns.Add(table, allIndexedColumns);
                    }
                    allIndexedColumns.Add(columns);
                }

                foreach (var entityType in model.GetEntityTypes())
                {
                    var table = entityType.GetTableName();
                    var pk = entityType.FindPrimaryKey();
                    if (pk == null)
                    {
                        // This is handled by the base validator.
                        continue;
                    }
                    var keyColumns = pk.Properties.Select(p => p.GetColumnName()).ToList();
                    if (tableKeyColumns.TryGetValue(table, out var allIndexedDbColumns))
                    {
                        if (!allIndexedDbColumns.Where(dbKeyColumns => Enumerable.SequenceEqual(keyColumns, dbKeyColumns)).Any())
                        {
                            throw new InvalidOperationException($"No primary key or other unique index was found in the database for table {table} for key column(s) ({string.Join(", ", keyColumns)}). {_disableValidationHint}");
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException($"No primary key was found in the database for table {table}. {_disableValidationHint}");
                    }
                }
            }
            catch (SpannerException e)
            {
                throw new InvalidOperationException($"Model validation against database failed. {_disableValidationHint}", e);
            }
        }
    }
}
