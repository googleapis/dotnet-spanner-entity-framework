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

using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal
{
    /// <summary>
    ///     This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerHistoryRepository : HistoryRepository
    {
        private static readonly TimeSpan _retryDelay = TimeSpan.FromSeconds(1);

        /// <summary>
        ///     The default name for the Migrations history table.
        /// </summary>
        public const string DefaultMigrationsHistoryTableName = "EFMigrationsHistory";

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public SpannerHistoryRepository(HistoryRepositoryDependencies dependencies)
            : base(dependencies)
        {
            Dependencies = dependencies;

            var relationalOptions = RelationalOptionsExtension.Extract(dependencies.Options);
            TableName = relationalOptions?.MigrationsHistoryTableName ?? DefaultMigrationsHistoryTableName;
        }

        protected override string TableName { get; }

        protected override HistoryRepositoryDependencies Dependencies { get; }

        private string CreateExistsSql(string tableName)
        {
            var stringTypeMapping = Dependencies.TypeMappingSource.GetMapping(typeof(string));

            var builder = new StringBuilder();
            builder.Append("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_catalog = '' and table_schema = '' and table_name = ")
                .Append($"{stringTypeMapping.GenerateSqlLiteral(Dependencies.SqlGenerationHelper.DelimitIdentifier(tableName, TableSchema))})");
            builder.Replace("`", "");
            return builder.ToString();
        }
        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        protected override string ExistsSql
        {
            get
            {
                return CreateExistsSql(TableName);
            }
        }

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        protected override bool InterpretExistsResult(object value) => value switch {
            long longValue => longValue != 0,
            bool boolValue => boolValue,
            _ => throw new ArgumentException(
                $"Unexpected type for EXISTS result: {value.GetType().Name}. Expected long or bool.", nameof(value))
        };

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetCreateIfNotExistsScript()
        {
            var script = GetCreateScript();
            return script.Insert(script.IndexOf("CREATE TABLE", StringComparison.Ordinal) + 12, " IF NOT EXISTS");
        }

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetBeginIfNotExistsScript(string migrationId)
        {
            throw new NotSupportedException("Cloud Spanner does not support CREATE IF NOT EXISTS style commands.");
        }
        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetBeginIfExistsScript(string migrationId)
        {
            throw new NotSupportedException("Cloud Spanner does not support CREATE IF NOT EXISTS style commands.");
        }

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetEndIfScript()
            => new StringBuilder()
                .Append("")
                .AppendLine(SqlGenerationHelper.StatementTerminator)
                .ToString();

        /// <summary>
        ///     The name of the table that will serve as a database-wide lock for migrations.
        /// </summary>
        protected virtual string LockTableName { get; } = "__EFMigrationsLock";

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Explicit;

        /// <summary>
        ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
        ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
        ///     any release. You should only use it directly in your code with extreme caution and knowing that
        ///     doing so can result in application failures when updating to a new Entity Framework Core release.
        /// </summary>
        public override IMigrationsDatabaseLock AcquireDatabaseLock()
        {
            Dependencies.MigrationsLogger.AcquiringMigrationLock();

            if (!InterpretExistsResult(
                Dependencies.RawSqlCommandBuilder.Build(CreateExistsSql(LockTableName))
                    .ExecuteScalar(CreateRelationalCommandParameters())))
            {
                CreateLockTableCommand().ExecuteNonQuery(CreateRelationalCommandParameters());
            }

            var retryDelay = _retryDelay;
            while (true)
            {
                long insertCount = 0;
                var dbLock = CreateMigrationDatabaseLock();
                var results = CreateInsertLockCommand(DateTimeOffset.UtcNow)
                    .ExecuteReader(CreateRelationalCommandParameters());

                if (results.Read())
                {
                    if (results.DbDataReader.FieldCount == 0 || results.DbDataReader.IsDBNull(0))
                    {
                        throw new InvalidOperationException("Failed to acquire migration lock.");
                    }

                    insertCount = results.DbDataReader.GetInt64(0);
                }
                if ((long)insertCount! == 1)
                {
                    return dbLock;
                }

                Thread.Sleep(retryDelay);
                if (retryDelay < TimeSpan.FromMinutes(1))
                {
                    retryDelay = retryDelay.Add(retryDelay);
                }
            }
        }

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public override async Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = default)
    {
        Dependencies.MigrationsLogger.AcquiringMigrationLock();

        if (!InterpretExistsResult(
            await Dependencies.RawSqlCommandBuilder.Build(CreateExistsSql(LockTableName))
                .ExecuteScalarAsync(CreateRelationalCommandParameters(), cancellationToken).ConfigureAwait(false)))
        {
            await CreateLockTableCommand().ExecuteNonQueryAsync(CreateRelationalCommandParameters(), cancellationToken)
                .ConfigureAwait(false);
        }

        var retryDelay = _retryDelay;
        while (true)
        {
            var dbLock = CreateMigrationDatabaseLock();
            var insertCount = await CreateInsertLockCommand(DateTimeOffset.UtcNow)
                .ExecuteScalarAsync(CreateRelationalCommandParameters(), cancellationToken)
                .ConfigureAwait(false);
            if ((long)insertCount! == 1)
            {
                return dbLock;
            }

            await Task.Delay(_retryDelay, cancellationToken).ConfigureAwait(true);
            if (retryDelay < TimeSpan.FromMinutes(1))
            {
                retryDelay = retryDelay.Add(retryDelay);
            }
        }
    }

    private IRelationalCommand CreateLockTableCommand()
        => Dependencies.RawSqlCommandBuilder.Build(
            $"""
CREATE TABLE IF NOT EXISTS "{LockTableName}" (
    "Id" INTEGER NOT NULL CONSTRAINT "PK_{LockTableName}" PRIMARY KEY,
    "Timestamp" TEXT NOT NULL
);
""");

    private IRelationalCommand CreateInsertLockCommand(DateTimeOffset timestamp)
    {
        var timestampLiteral = Dependencies.TypeMappingSource.GetMapping(typeof(DateTimeOffset)).GenerateSqlLiteral(timestamp);

        return Dependencies.RawSqlCommandBuilder.Build(
            $"""
INSERT OR IGNORE INTO "{LockTableName}"("Id", "Timestamp") VALUES(1, {timestampLiteral});
SELECT changes();
""");
    }

    private IRelationalCommand CreateDeleteLockCommand(int? id = null)
    {
        var sql = $"""
DELETE FROM "{LockTableName}"
""";
        if (id != null)
        {
            sql += $""" WHERE "Id" = {id}""";
        }

        sql += ";";
        return Dependencies.RawSqlCommandBuilder.Build(sql);
    }

    private SpannerMigrationDatabaseLock CreateMigrationDatabaseLock()
        => new(CreateDeleteLockCommand(), CreateRelationalCommandParameters(), this);

    private RelationalCommandParameterObject CreateRelationalCommandParameters()
        => new(
            Dependencies.Connection,
            null,
            null,
            Dependencies.CurrentContext.Context,
            Dependencies.CommandLogger, CommandSource.Migrations);
    }
}
