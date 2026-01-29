// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Api.Gax;
using Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal;
using Google.Cloud.Spanner.Data;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Cloud.EntityFrameworkCore.Spanner.Infrastructure;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal
{
    internal class SpannerMigrationCommandExecutor : IMigrationCommandExecutor
    {
        public void ExecuteNonQuery(IEnumerable<MigrationCommand> migrationCommands, IRelationalConnection connection)
        {
            ExecuteNonQueryAsync(migrationCommands, connection).WaitWithUnwrappedExceptions();
        }

        public async Task ExecuteNonQueryAsync(IEnumerable<MigrationCommand> migrationCommands, IRelationalConnection connection, CancellationToken cancellationToken = default)
        {
            GaxPreconditions.CheckArgument(connection is SpannerRelationalConnection, nameof(connection), "Can only be used with Spanner connections");
            var statements = migrationCommands.Select(x => x.CommandText).ToArray();
            if (statements.Length == 0)
            {
                return;
            }
            var ddlStatements = statements.Where(IsDdlStatement).ToArray();
            var otherStatements = statements.Where(x => !IsDdlStatement(x)).ToList();
            var spannerConnection = (((SpannerRelationalConnection) connection).DbConnection as SpannerRetriableConnection)!;
            if (ddlStatements.Any())
            {
                var cmd = spannerConnection.CreateDdlCommand(ddlStatements[0], ddlStatements.Skip(1).ToArray());
                var spannerRelationalConnection = connection as SpannerRelationalConnection;
                if (spannerRelationalConnection?.DdlExecutionStrategy == DdlExecutionStrategy.StartOperation)
                {
                    await cmd.StartDdlOperationAsync(cancellationToken);
                }
                else
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
            if (otherStatements.Any())
            {
                await using var transaction = await spannerConnection.BeginTransactionAsync(cancellationToken);
                var cmd = spannerConnection.CreateBatchDmlCommand();
                cmd.Transaction = transaction;
                foreach (var statement in otherStatements)
                {
                    cmd.Add(statement);
                }
                await cmd.ExecuteNonQueryAsync(cancellationToken);
                await transaction.CommitAsync(cancellationToken);
            }
        }

        private bool IsDdlStatement(string statement)
        {
            return SpannerCommandTextBuilder.FromCommandText(statement).SpannerCommandType == SpannerCommandType.Ddl;
        }
    }
}
