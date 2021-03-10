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
using Google.Cloud.Spanner.Admin.Database.V1;
using Grpc.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Storage.Internal
{
    /// <summary>
    /// This is internal functionality and not intended for public use.
    /// </summary>
    public class SpannerDatabaseCreator : RelationalDatabaseCreator
    {
        //Note: This creator is used for migration when the developer calls Create and Delete on a DbContext.

        private readonly ISpannerRelationalConnection _connection;
        private readonly IRawSqlCommandBuilder _rawSqlCommandBuilder;

        /// <summary>
        /// This is internal functionality and not intended for public use.
        /// </summary>
        public SpannerDatabaseCreator(
            RelationalDatabaseCreatorDependencies dependencies,
            ISpannerRelationalConnection connection,
            IRawSqlCommandBuilder rawSqlCommandBuilder)
            : base(dependencies)
        {
            _connection = connection;
            _rawSqlCommandBuilder = rawSqlCommandBuilder;
        }

        /// <summary>
        /// </summary>
        public override void Create()
        {
            using var masterConnection = _connection.CreateMasterConnection();
            Dependencies.MigrationCommandExecutor
                .ExecuteNonQuery(CreateCreateOperations(), masterConnection);
        }


        /// <inheritdoc />
        public override async Task CreateAsync(CancellationToken cancellationToken = default)
        {
            using var masterConnection = _connection.CreateMasterConnection();
            await Dependencies.MigrationCommandExecutor
                .ExecuteNonQueryAsync(CreateCreateOperations(), masterConnection, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override bool HasTables()
            => Dependencies.ExecutionStrategyFactory
                .Create()
                .Execute(
                    _connection,
                    connection => (bool)CreateHasTablesCommand()
                            .ExecuteScalar(
                                new RelationalCommandParameterObject(
                                    connection,
                                    null,
                                    null,
                                    Dependencies.CurrentContext.Context,
                                    Dependencies.CommandLogger)));

        /// <inheritdoc />
        public override Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
            => Dependencies.ExecutionStrategyFactory.Create().ExecuteAsync(
                _connection,
                async (connection, ct) => (bool)await CreateHasTablesCommand()
                        .ExecuteScalarAsync(
                            new RelationalCommandParameterObject(
                                connection,
                                null,
                                null,
                                Dependencies.CurrentContext.Context,
                                Dependencies.CommandLogger),
                            cancellationToken: ct), cancellationToken);

        private IRelationalCommand CreateHasTablesCommand()
            => _rawSqlCommandBuilder
                .Build(@"
                    SELECT CASE WHEN COUNT(*) = 0 THEN FALSE ELSE TRUE END
                    FROM information_schema.tables AS t
                    WHERE t.table_catalog = '' and t.table_schema = ''
                ");

        private IReadOnlyList<MigrationCommand> CreateCreateOperations()
            => Dependencies.MigrationsSqlGenerator.Generate(new[]
            {
                new SpannerCreateDatabaseOperation
                {
                    Name = _connection.DbConnection.Database
                }
            });

        /// <inheritdoc />
        public override bool Exists() => Task.Run(() => ExistsAsync()).ResultWithUnwrappedExceptions();

        /// <inheritdoc />
        public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                DatabaseAdminClient databaseAdminClient = DatabaseAdminClient.Create();
                await databaseAdminClient.GetDatabaseAsync(_connection.DbConnection.DataSource, cancellationToken);
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {
                return false;
            }
            return true;
        }

        /// <inheritdoc />
        public override void Delete()
        {
            using var masterConnection = _connection.CreateMasterConnection();
            Dependencies.MigrationCommandExecutor
                .ExecuteNonQuery(CreateDropCommands(), masterConnection);
        }

        /// <inheritdoc />
        public override async Task DeleteAsync(CancellationToken cancellationToken = default)
        {
            using var masterConnection = _connection.CreateMasterConnection();
            await Dependencies.MigrationCommandExecutor
                .ExecuteNonQueryAsync(CreateDropCommands(), masterConnection, cancellationToken)
                .ConfigureAwait(false);
        }

        /// <inheritdoc />
        public override void CreateTables()
        {
            var operations = Dependencies.ModelDiffer.GetDifferences(null, Dependencies.Model);
            var commands = Dependencies.MigrationsSqlGenerator.Generate(operations, Dependencies.Model);

            Dependencies.MigrationCommandExecutor.ExecuteNonQuery(commands, _connection);
        }

        /// <inheritdoc />
        public override async Task CreateTablesAsync(CancellationToken cancellationToken = default)
        {
            var operations = Dependencies.ModelDiffer.GetDifferences(null, Dependencies.Model);
            var commands = Dependencies.MigrationsSqlGenerator.Generate(operations, Dependencies.Model);

            await Dependencies.MigrationCommandExecutor.ExecuteNonQueryAsync(commands, _connection,
                cancellationToken).ConfigureAwait(false);
        }

        private IReadOnlyList<MigrationCommand> CreateDropCommands()
        {
            var operations = new MigrationOperation[]
            {
                new SpannerDropDatabaseOperation {Name = _connection.DbConnection.Database}
            };

            return Dependencies.MigrationsSqlGenerator.Generate(operations);
        }
    }
}
