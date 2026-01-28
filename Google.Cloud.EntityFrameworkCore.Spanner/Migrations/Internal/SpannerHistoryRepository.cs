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
            TableName = relationalOptions?.MigrationsHistoryTableName ?? DefaultMigrationsHistoryTableName;
        }

        protected override string TableName { get; }

        protected override HistoryRepositoryDependencies Dependencies { get; }

        public override LockReleaseBehavior LockReleaseBehavior { get; } = LockReleaseBehavior.Explicit;

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


        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        protected override string ExistsSql =>
            $"SELECT EXISTS(SELECT 1 " +
            $"FROM information_schema.tables " +
            $"WHERE table_schema = '{TableSchema}' " +
            $"  AND table_name = '{TableName}')";

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        protected override bool InterpretExistsResult(object value) => (bool)value;

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
    }
}
