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
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.IO;
using System.Text;

namespace Google.Cloud.EntityFrameworkCore.Spanner.Migrations.Internal
{
    public class SpannerHistoryRepository : HistoryRepository
    {

        private const string DefaultMigrationsHistoryTableName = "EFMigrationsHistory";
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

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        protected override string ExistsSql
        {
            get
            {
                var stringTypeMapping = Dependencies.TypeMappingSource.GetMapping(typeof(string));

                var builder = new StringBuilder();
                builder.Append("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_catalog = '' and table_schema = '' and table_name = ")
                    .Append($"{stringTypeMapping.GenerateSqlLiteral(SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema))})");
                return builder.ToString();
            }
        }

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        protected override bool InterpretExistsResult(object value) => (bool)value;

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetCreateIfNotExistsScript()
        {
            // TODO: find specific use case and create query for same.
            throw new NotImplementedException();
        }

        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetBeginIfNotExistsScript(string migrationId)
        {
            // TODO: find specific use case and create query for same.
            throw new NotImplementedException();
        }
        /// <summary>
        ///     This is internal functionality and not intended for public use.
        /// </summary>
        public override string GetBeginIfExistsScript(string migrationId)
        {
            // TODO: find specific use case and create query for same.
            throw new NotImplementedException();
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
