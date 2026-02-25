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

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal
{
    internal sealed class NoOpMigrationsDatabaseLock : IMigrationsDatabaseLock
    {
        public IHistoryRepository HistoryRepository { get; }

        internal NoOpMigrationsDatabaseLock(IHistoryRepository historyRepository)
        {
            HistoryRepository = historyRepository;
        }
        
        public void Dispose()
        {
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
    
    /// <summary>
    ///     This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerHistoryRepository : HistoryRepository
    {
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
            TableName = relationalOptions.MigrationsHistoryTableName ?? DefaultMigrationsHistoryTableName;
        }

        protected override string TableName { get; }

        protected override HistoryRepositoryDependencies Dependencies { get; }

        public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Explicit;

        public override IMigrationsDatabaseLock AcquireDatabaseLock()
        {
            // Spanner does not have a feature that can be used to exclusively lock the entire database.
            // So we translate this to a no-op.
            return new NoOpMigrationsDatabaseLock(this);
        }

        public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(AcquireDatabaseLock());
        }

        public override bool Exists()
        {
            if (!Dependencies.DatabaseCreator.Exists())
            {
                return false;
            }
            var connection = Dependencies.Connection.DbConnection;
            // Unwrap the underlying Spanner connection, so we can execute the query outside any transaction
            // that might be registered on the connection that is used by Entity Framework. We need to do this,
            // as Spanner does not allow queries on INFORMATION_SCHEMA in a transaction.
            if (connection is SpannerRetriableConnection spannerConnection)
            {
                using var command = spannerConnection.SpannerConnection.CreateSelectCommand(ExistsSql);
                using var reader = command.ExecuteReader();
                if (reader.Read())
                {
                    return reader.GetBoolean(0);
                }
            }
            return false;
        }

        public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            if (!await Dependencies.DatabaseCreator.ExistsAsync(cancellationToken))
            {
                return false;
            }
            var connection = Dependencies.Connection.DbConnection;
            // Unwrap the underlying Spanner connection, so we can execute the query outside any transaction
            // that might be registered on the connection that is used by Entity Framework. We need to do this,
            // as Spanner does not allow queries on INFORMATION_SCHEMA in a transaction.
            if (connection is SpannerRetriableConnection spannerConnection)
            {
                await using var command = spannerConnection.SpannerConnection.CreateSelectCommand(ExistsSql);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken))
                {
                    return reader.GetBoolean(0);
                }
            }
            return false;
        }

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        protected override string ExistsSql =>
            $"SELECT EXISTS(SELECT 1 " +
            $"FROM information_schema.tables " +
            $"WHERE table_schema = '{TableSchema}' " +
            $"    AND table_name = '{TableName}')";

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        protected override bool InterpretExistsResult(object value) => value != null && (bool)value;

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetCreateIfNotExistsScript()
        {
            return GetCreateScript().Replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
        }

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetBeginIfNotExistsScript(string migrationId)
        {
            throw new NotSupportedException("Cloud Spanner does not support conditional SQL execution blocks.");
        }
        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetBeginIfExistsScript(string migrationId)
        {
            throw new NotSupportedException("Cloud Spanner does not support conditional SQL execution blocks.");
        }

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetEndIfScript()
            => new StringBuilder()
                .Append("")
                .AppendLine(SqlGenerationHelper.StatementTerminator)
                .ToString();
    }
}
